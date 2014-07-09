/***************************************************************************
 *   Copyright (C) 2012 by H-Store Project                                 *
 *   Brown University                                                      *
 *   Massachusetts Institute of Technology                                 *
 *   Yale University                                                       *
 *                                                                         *
 *   Permission is hereby granted, free of charge, to any person obtaining *
 *   a copy of this software and associated documentation files (the       *
 *   "Software"), to deal in the Software without restriction, including   *
 *   without limitation the rights to use, copy, modify, merge, publish,   *
 *   distribute, sublicense, and/or sell copies of the Software, and to    *
 *   permit persons to whom the Software is furnished to do so, subject to *
 *   the following conditions:                                             *
 *                                                                         *
 *   The above copyright notice and this permission notice shall be        *
 *   included in all copies or substantial portions of the Software.       *
 *                                                                         *
 *   THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,       *
 *   EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF    *
 *   MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.*
 *   IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR     *
 *   OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, *
 *   ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR *
 *   OTHER DEALINGS IN THE SOFTWARE.                                       *
 ***************************************************************************/
package edu.brown.hstore.cmdlog;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.log4j.Logger;
import org.voltdb.CatalogContext;
import org.voltdb.ClientResponseImpl;
import org.voltdb.catalog.Procedure;
import org.voltdb.exceptions.ServerFaultException;
import org.voltdb.messaging.FastSerializer;
import org.voltdb.utils.CompressionService;
import org.voltdb.utils.DBBPool.BBContainer;

import com.google.protobuf.RpcCallback;

import edu.brown.hstore.HStoreConstants;
import edu.brown.hstore.HStoreSite;
import edu.brown.hstore.HStoreThreadManager;
import edu.brown.hstore.conf.HStoreConf;
import edu.brown.hstore.txns.AbstractTransaction;
import edu.brown.hstore.txns.LocalTransaction;
import edu.brown.interfaces.Shutdownable;
import edu.brown.logging.LoggerUtil;
import edu.brown.logging.LoggerUtil.LoggerBoolean;
import edu.brown.profilers.CommandLogWriterProfiler;
import edu.brown.profilers.ProfileMeasurementUtil;
import edu.brown.utils.ExceptionHandlingRunnable;
import edu.brown.utils.StringUtil;

/**
 * Transaction Command Log Writer
 * 
 * @author mkirsch
 * @author pavlo
 * @author debrabant
 */
public class CommandLogWriter extends ExceptionHandlingRunnable implements Shutdownable {
    private static final Logger LOG = Logger.getLogger(CommandLogWriter.class);
    private static final LoggerBoolean debug = new LoggerBoolean();
    private static final LoggerBoolean trace = new LoggerBoolean();
    static {
        LoggerUtil.attachObserver(LOG, debug, trace);
    }
    
    /**
     * The default file extension to use for the command log output
     */
    public static final String LOG_OUTPUT_EXT = ".cmdlog"; 
    
    /**
     * Special LogEntry that holds additional data that we need in order to send
     * back a ClientResponse
     */
    protected class WriterLogEntry extends LogEntry {
        protected ClientResponseImpl cresponse;
        protected RpcCallback<ClientResponseImpl> clientCallback;
        protected long initiateTime;
        protected int restartCounter;

        public LogEntry init(LocalTransaction ts, ClientResponseImpl cresponse) {
            this.cresponse = cresponse;
            this.clientCallback = ts.getClientCallback();
            this.initiateTime = ts.getInitiateTime();
            this.restartCounter = ts.getRestartCounter();
            return super.init(ts);
        }

        @Override
        public void finish() {
            super.finish();
            this.cresponse = null;
            this.clientCallback = null;
            this.initiateTime = -1;
            this.restartCounter = -1;
        }
    }

    /**
     * Circular Buffer of Log Entries
     */
    protected class CircularLogEntryBuffer {
        private final WriterLogEntry buffer[];
        private int startPos;
        private int nextPos;

        public CircularLogEntryBuffer(int size) {
            size += 1; // hack to make wrapping around work
            this.buffer = new WriterLogEntry[size];
            for (int i = 0; i < size; i++) {
                this.buffer[i] = new WriterLogEntry();
            } // FOR
            this.startPos = 0;
            this.nextPos = 0;
        }

        public LogEntry next(LocalTransaction ts, ClientResponseImpl cresponse) {
            // Check that they don't try add the same txn twice right after each
            // other
            if (hstore_conf.site.jvm_asserts) {
                LogEntry prev = this.buffer[this.previous()];
                if (prev.isInitialized()) {
                    assert (ts.getTransactionId().equals(prev.getTransactionId()) == false) : String.format("Trying to queue %s in the %s twice\n%s", ts, CommandLogWriter.class.getSimpleName(), prev);
                }
            }

            // The internal pointer to the next element does not need to be
            // atomic because
            // we are going to maintain separate buffers for each partition.
            // But we need to think about what happens if we are about to wrap
            // around and we
            // haven't been flushed to disk yet.
            LogEntry ret = this.buffer[this.nextPos].init(ts, cresponse);
            this.nextPos = (this.nextPos + 1) % this.buffer.length;
            ;
            return ret;
        }

        public void flushCleanup() {
            // for (int i = 0; i < this.getSize(); i++)
            // this.buffer[(this.startPos + i) % this.buffer.length].finish();
            this.startPos = this.nextPos;
        }

        public int getStart() {
            return this.startPos;
        }

        public int size() {
            return ((this.nextPos + this.buffer.length) - this.startPos) % this.buffer.length;
        }

        private int previous() {
            return ((this.nextPos == 0 ? this.buffer.length : this.nextPos) - 1);
        }

        @Override
        public String toString() {
            return String.format("%s[start=%d / next=%s]@%d", this.getClass().getSimpleName(), this.startPos, this.nextPos, this.hashCode());
        }
    } // CLASS

    private final HStoreSite hstore_site;
    private final HStoreConf hstore_conf;
    private final CatalogContext catalogContext;
    private final File outputFile;
    private final FileChannel fstream;

    private final Semaphore writingEntry;
    private final int numWritingLocks;

    private final boolean useGroupCommit;
    private boolean usePostProcessor;
    private final int group_commit_size;
    private final FastSerializer singletonSerializer;
    private final LogEntry singletonLogEntry;

    private int commitBatchCounter = 0;
    private boolean stop = false;
    private Thread self;

    /**
     * If set to true, then the WriterThread is in the middle of writing out
     * data to the log file.
     */
    private final AtomicBoolean flushInProgress = new AtomicBoolean(false);

    /**
     * The log entry buffers (one per partition)
     */
    private CircularLogEntryBuffer entries[];
    private CircularLogEntryBuffer entriesFlushing[];

    private CommandLogWriterProfiler profiler;

    /**
     * Constructor
     * 
     * @param catalog_db
     * @param path
     */
    public CommandLogWriter(HStoreSite hstore_site, File outputFile) {
        this.hstore_site = hstore_site;
        this.hstore_conf = hstore_site.getHStoreConf();
        this.catalogContext = hstore_site.getCatalogContext();
        this.outputFile = outputFile;
        this.singletonSerializer = new FastSerializer(true, true);
        // this.group_commit_size = Math.max(1,
        // hstore_conf.site.exec_command_logging_group_commit); //Group commit
        // threshold, or 1 if group commit is turned off

        // Number of local partitions
        int num_partitions = hstore_site.getLocalPartitionIds().size();
        this.numWritingLocks = num_partitions;

        // Number of log entries per partition
        // hack, set arbitrarily high to avoid contention for log buffer
        int num_entries = Math.max(10000, hstore_conf.site.network_incoming_limit_txns);

        // The global number of txns we will commit in a batch
        this.group_commit_size = num_entries * num_partitions;

        if (debug.val) {
            LOG.debug("group_commit_size: " + this.group_commit_size);
            LOG.debug("group_commit_timeout: " + hstore_conf.site.commandlog_timeout);
        }

        // Configure group commit parameters
        if (this.group_commit_size > 0) {
            this.useGroupCommit = true;

            // Make one entry buffer per partition SO THAT SYNCHRONIZATION ON
            // EACH BUFFER IS NOT REQUIRED
            this.writingEntry = new Semaphore(this.numWritingLocks, false);
            this.entries = new CircularLogEntryBuffer[num_partitions];
            this.entriesFlushing = new CircularLogEntryBuffer[num_partitions];
            for (int partition = 0; partition < num_partitions; partition++) {
                this.entries[partition] = new CircularLogEntryBuffer(num_entries);
                this.entriesFlushing[partition] = new CircularLogEntryBuffer(num_entries);
            } // FOR
            this.singletonLogEntry = null;
        } else {
            this.useGroupCommit = false;
            this.writingEntry = null;
            this.singletonLogEntry = new LogEntry();
        }

        FileOutputStream f = null;
        try {
            this.outputFile.getParentFile().mkdirs();
            LOG.info("Command Log File: " + this.outputFile.getAbsolutePath());
            this.outputFile.createNewFile();
            f = new FileOutputStream(this.outputFile, false);
        } catch (IOException ex) {
            throw new RuntimeException(ex);
        }
        this.fstream = f.getChannel();

        // Write out a header to the file
        this.writeHeader();

        // Writer Profiling
        if (hstore_conf.site.commandlog_profiling) {
            this.profiler = new CommandLogWriterProfiler();
        }
    }

    /**
     * Separate thread for writing out entries to the log
     */
    @Override
    public void runImpl() {
        this.self = Thread.currentThread();
        this.self.setName(HStoreThreadManager.getThreadName(hstore_site, HStoreConstants.THREAD_NAME_COMMANDLOGGER));
        this.hstore_site.getThreadManager().registerProcessingThread();

        this.usePostProcessor = hstore_site.hasTransactionPostProcessors();

        CircularLogEntryBuffer temp[] = null;
        long next = System.currentTimeMillis() + hstore_conf.site.commandlog_timeout;
        while (this.stop == false) {
            // Sleep until our timeout period, at which point a
            // flush will be initiated
            try {
                long sleep = Math.max(0, System.currentTimeMillis() - next);
                Thread.sleep(sleep);
            } catch (InterruptedException e) {
                if (this.stop)
                    break;
            } finally {
                next = System.currentTimeMillis() + hstore_conf.site.commandlog_timeout;
                // if (debug.val)
                // LOG.debug("Group commit timeout occurred, writing buffer to disk.");
            }

            // Take all of the writing permits. This will stop any other
            // thread from appending to the buffer that we're about to swap
            int free_permits = this.numWritingLocks - this.writingEntry.drainPermits();
            if (free_permits > 0) {
                if (trace.val)
                    LOG.trace("Acquiring " + free_permits + " this.writeEntry permits");
                do {
                    try {
                        writingEntry.acquire(free_permits);
                    } catch (InterruptedException ex) {
                        continue;
                    }
                    break;
                } while (this.stop == false);
            }

            // At this point we know that nobody else could be writing to the
            // current buffer for the threads, so it's safe for us to swap it
            // with the one that we just wrote out to disk
            // SYNC POINT: a synchronization point between the thread
            // filling the buffer and the writing thread where a full
            // buffer is exchanged for an empty one and the full
            // buffer is written out to disk.
            temp = this.entries;
            this.entries = this.entriesFlushing;
            this.entriesFlushing = temp;
            assert (this.entries != this.entriesFlushing);

            // Release our entry permits so that other threads can
            // start filling up their Entry buffers
            // if (trace.val) LOG.trace("Releasing writingEntry permits");
            this.writingEntry.release(this.group_commit_size);

            // Write the entries out to disk
            // if (debug.val) LOG.debug("Executing group commit");
            this.flushInProgress.set(true);
            if (this.groupCommit(this.entriesFlushing) == 0) {
                next = System.currentTimeMillis() + hstore_conf.site.commandlog_timeout;
            }
            this.flushInProgress.lazySet(false);
        } // WHILE
    }

    @Override
    public void prepareShutdown(boolean error) {
        this.stop = true;
    }

    /**
     * Force the writer thread to flush all entries out to disk right now.
     * Multiple invocations of this will not be queued
     */
    protected void flush() throws InterruptedException {
        // Wait until it starts running
        while (this.flushInProgress.get() == false) {
            // this.flushThread.interrupt();
            Thread.yield();
        } // WHILE

        // Then wait until it's done running
        while (this.flushInProgress.get()) {
            Thread.yield();
        } // WHILE
    }

    /**
     * Get the total number of txns that are queued within this object.
     * <B>Note:</B> This is not thread-safe because the writer thread may swap
     * the entries in the middle of the count calculation.
     * 
     * @return
     */
    public int getTotalTxnCount() {
        int total = 0;
        for (CircularLogEntryBuffer c : this.entries) {
            total += c.size();
        } // FOR
        for (CircularLogEntryBuffer c : this.entriesFlushing) {
            total += c.size();
        } // FOR
        return (total);
    }

    @Override
    public void shutdown() {
        if (this.self != null) {
            this.stop = true;
            while (this.self.isAlive()) {
                Thread.yield();
            } // WHILE

            if (debug.val) {
                Map<String, Object> m = new LinkedHashMap<String, Object>();
                m.put("Current Buffer", StringUtil.join("\n", this.entries));
                m.put("Flushing Buffer", StringUtil.join("\n", this.entriesFlushing));
                LOG.debug("Closing WAL file\n" + StringUtil.formatMaps(m).trim() + " File :" + this.outputFile.getAbsolutePath());
            }
        }
        try {
            LOG.trace("Closing stream  :: size :" + this.fstream.size());
            
            this.fstream.close();
                
        } catch (IOException ex) {
            String message = "Failed to close WAL file";
            throw new ServerFaultException(message, ex);
        }
    }

    @Override
    public boolean isShuttingDown() {
        return (this.stop);
    }

    public CommandLogWriterProfiler getProfiler() {
        return this.profiler;
    }

    public boolean writeHeader() {
        assert (this.singletonSerializer != null);
        try {
            this.singletonSerializer.clear();
            this.singletonSerializer.writeBoolean(this.group_commit_size > 0);// Using
                                                                              // group
                                                                              // commit
            this.singletonSerializer.writeInt(this.catalogContext.procedures.size());
            for (Procedure catalog_proc : this.catalogContext.procedures.values()) {
                int procId = catalog_proc.getId();
                this.singletonSerializer.writeInt(procId);
                this.singletonSerializer.writeString(catalog_proc.getName());
            } // FOR

            BBContainer b = this.singletonSerializer.getBBContainer();
            this.fstream.write(b.b.asReadOnlyBuffer());
            this.fstream.force(true);
        } catch (Exception e) {
            String message = "Failed to write log headers";
            throw new ServerFaultException(message, e);
        }
    
        return (true);
    }
    
    /**
     * GroupCommits the given buffer set all at once
     * 
     * @param eb
     */
    public int groupCommit(CircularLogEntryBuffer[] eb) {
        if (hstore_conf.site.commandlog_profiling) {
            if (this.profiler == null)
                this.profiler = new CommandLogWriterProfiler();
            this.profiler.writingTime.start();
        }

        // Write all to a single FastSerializer buffer
        this.singletonSerializer.clear();
        int txnCounter = 0;
        for (int i = 0; i < eb.length; i++) {
            try {
                assert (this.singletonSerializer != null);
                int size = eb[i].buffer.length;
                int position = eb[i].startPos;
                while (position != eb[i].nextPos) {
                    WriterLogEntry entry = eb[i].buffer[position++];
                    try {
                        this.singletonSerializer.writeObject(entry);
                        txnCounter++;
                    } catch (Throwable ex) {
                        LOG.warn("Failed to write log entry", ex);
                    }
                    if (debug.val)
                        LOG.debug(String.format("Prepared txn #%d for group commit batch #%d", entry.getTransactionId(), this.commitBatchCounter));
                    if (position >= size)
                        position = 0;
                } // WHILE
            } catch (Exception e) {
                String message = "Failed to serialize buffer during group commit";
                throw new ServerFaultException(message, e);
            }
        } // FOR
        if (txnCounter == 0) {
            // if (debug.val)
            // LOG.debug("No transactions are in the current buffers. Not writing anything to disk");
            return (txnCounter);
        }
        
        // Compress and force out to disk
        ByteBuffer compressed;
        try {
            compressed = CompressionService.compressBufferForMessaging(this.singletonSerializer.getBBContainer().b);
        } catch (IOException e) {
            throw new RuntimeException("Failed to compress WAL buffer");
        }
        
        if (debug.val)
            LOG.debug(String.format("Writing out %d bytes for %d txns [batchCtr=%d]", compressed.limit(), txnCounter, this.commitBatchCounter));
        try {
            this.fstream.write(compressed);
            this.fstream.force(true);
        } catch (IOException ex) {
            ex.printStackTrace();
            String message = "Failed to group commit for buffer";
            throw new ServerFaultException(message, ex);
        }
        if (hstore_conf.site.commandlog_profiling && profiler != null)
            ProfileMeasurementUtil.swap(profiler.writingTime, profiler.networkTime);
        try {
            // Send responses
            for (int i = 0; i < eb.length; i++) {
                CircularLogEntryBuffer buffer = eb[i];
                int start = buffer.getStart();
                for (int j = 0, size = buffer.size(); j < size; j++) {
                    WriterLogEntry entry = buffer.buffer[(start + j) % buffer.buffer.length];
                    if (entry.isInitialized()) {
                        if (this.usePostProcessor) {
                            hstore_site.responseQueue(entry.cresponse, entry.clientCallback, entry.initiateTime, entry.restartCounter);
                        } else {
                            hstore_site.responseSend(entry.cresponse, entry.clientCallback, entry.initiateTime, entry.restartCounter);
                        }
                    } else {
                        LOG.warn("Unexpected unintialized " + entry.getClass().getSimpleName());
                    }

                } // FOR
                buffer.flushCleanup();
            } // FOR
        } finally {
            if (hstore_conf.site.commandlog_profiling && profiler != null)
                profiler.networkTime.stop();
        }

        this.commitBatchCounter++;
        return (txnCounter);
    }
    
    /**
     * Write a completed transaction handle out to the WAL file. Returns true if
     * the entry has been successfully written to disk and the HStoreSite needs
     * to send out the ClientResponse
     * @param ts
     * @return
     */
    public boolean appendToLog(final LocalTransaction ts, final ClientResponseImpl cresponse) {
        boolean sendResponse = true;

        // -------------------------------
        // QUEUE FOR GROUP COMMIT
        // -------------------------------
        if (this.useGroupCommit) {
            if (trace.val)
                LOG.trace(ts + " - Attempting to queue txn to write out to command log using group commit");
        
            int basePartition = ts.getBasePartition();
            assert(this.hstore_site.isLocalPartition(basePartition));
            int offset = this.hstore_site.getLocalPartitionOffset(basePartition);

            // get the buffer for the partition of the current transaction
            CircularLogEntryBuffer buffer = this.entries[offset];
            assert(buffer != null) : "Missing log entry buffer for partition " + basePartition;
            try {
                // acquire semaphore permit to write a transaction to the log
                // buffer will wait if buffer is currently being swapped
                this.writingEntry.acquire();

                // create an entry for this transaction in the buffer for this partition
                // NOTE: this is guaranteed to be thread-safe because there is
                // only one thread per partition
                LogEntry entry = buffer.next(ts, cresponse);
                assert(entry != null);
                if (trace.val)
                    LOG.trace(String.format("New %s %s from %s for partition %d", entry.getClass().getSimpleName(), entry, buffer, basePartition));

                this.writingEntry.release();
            } catch (InterruptedException e) {
                throw new RuntimeException("Unexpected interruption while waiting for WriterThread to finish");
            } finally {
                if (hstore_conf.site.commandlog_profiling && profiler != null) profiler.blockedTime.stopIfStarted();
            }

            if (trace.val)
                LOG.trace(ts + " - Finished queuing txn to write out to command log");
            
            // We always want to set this to false because our flush thread will
            // be the one that actually sends out the network messages
            sendResponse = false;
        }
        // -------------------------------
        // NO GROUP COMMIT -- FINISH AND RETURN TRUE
        // -------------------------------
        else { 
            try {
                FastSerializer fs = this.singletonSerializer;
                assert (fs != null);
                fs.clear();
                this.singletonLogEntry.init(ts);
                fs.writeObject(this.singletonLogEntry);
                BBContainer b = fs.getBBContainer();
                this.fstream.write(b.b.asReadOnlyBuffer());
                this.fstream.force(true);
                this.singletonLogEntry.finish();
            } catch (Exception e) {
                String message = "Failed to write single log entry for " + ts.toString();
                throw new ServerFaultException(message, e, ts.getTransactionId());
            }
        }
        
        return (sendResponse);
    }
}
