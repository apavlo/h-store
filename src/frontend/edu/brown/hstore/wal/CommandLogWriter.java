/***************************************************************************
 *   Copyright (C) 2011 by H-Store Project                                 *
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
package edu.brown.hstore.wal;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.Exchanger;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.log4j.Logger;
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
import edu.brown.hstore.interfaces.Shutdownable;
import edu.brown.hstore.txns.LocalTransaction;
import edu.brown.logging.LoggerUtil;
import edu.brown.logging.LoggerUtil.LoggerBoolean;
import edu.brown.utils.ProfileMeasurement;

/**
 * Transaction Command Log Writer
 * @author mkirsch
 * @author pavlo
 * @author debrabant
 */
public class CommandLogWriter implements Shutdownable {
    private static final Logger LOG = Logger.getLogger(CommandLogWriter.class);
    
    private final static LoggerBoolean debug = new LoggerBoolean(LOG.isDebugEnabled());
    private final static LoggerBoolean trace = new LoggerBoolean(LOG.isTraceEnabled());
    
    static {
        LoggerUtil.attachObserver(LOG, debug, trace);
    }
    
    /**
     * Special LogEntry that holds additional data that we
     * need in order to send back a ClientResponse
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
    protected class EntryBuffer {
        private final FastSerializer fs;
        private final WriterLogEntry buffer[];
        private int startPos;
        private int nextPos;
        
        public EntryBuffer(int size, FastSerializer serializer) {
            size += 1; //hack to make wrapping around work
            this.fs = serializer;
            this.buffer = new WriterLogEntry[size];
            for (int i = 0; i < size; i++) {
                this.buffer[i] = new WriterLogEntry();
            } // FOR
            startPos = 0;
            nextPos = 0; 
        }
        public FastSerializer getSerializer() {
            return this.fs;
        }
        public LogEntry next(LocalTransaction ts, ClientResponseImpl cresponse) {
            // TODO: The internal pointer to the next element does not need to be atomic
            // But we need to think about what happens if we are about to wrap around and we
            // haven't been flushed to disk yet.
            LogEntry ret = this.buffer[nextPos].init(ts, cresponse); 
            nextPos = (nextPos + 1) % this.buffer.length;;
            return ret;
        }
        public void flushCleanup() {
            //for (int i = 0; i < this.getSize(); i++)
            //this.buffer[(this.startPos + i) % this.buffer.length].finish();
            this.startPos = this.nextPos;
        }
        public int getStart() {
            return startPos;
        }
        public int getSize() {
            return ((nextPos + this.buffer.length) - startPos) % this.buffer.length;
        }
    } // CLASS
    
    /**
     * Separate thread for writing out entries to the log
     */
    protected class WriterThread extends Thread {
        {
            this.setDaemon(true);
        }
        
        @Override
        public void run() {
            Thread self = Thread.currentThread();
            self.setName(HStoreThreadManager.getThreadName(hstore_site, HStoreConstants.THREAD_NAME_COMMANDLOGGER));
            
            while (!stop) {
                try 
                {
                    // This sync point is designed to timeout, at which point a flush will be initiated 
                    entriesFlushing = bufferExchange.exchange(entriesFlushing, 
                                                              hstore_conf.site.exec_command_logging_group_commit_timeout, 
                                                              TimeUnit.MILLISECONDS);
                    
                } 
                catch (InterruptedException e) 
                {
                    throw new RuntimeException("WAL writer thread interrupted while waiting for a new buffer" + e.getStackTrace().toString());
                }
                // this will be thrown when the group commit buffer was not filled before the group_commit_timeout
                catch (TimeoutException e) 
                { 
                    LOG.info("Group commit timeout occurred, writing buffer to disk."); 
                    
                    try 
                    {
                        flushInProgress.set(true);
                        swapBuffers.set(true); 
                                                
                        int free_permits = group_commit_size - writingEntry.drainPermits(); 
                        
                        writingEntry.acquire(free_permits); 
                        
                        // SYNC POINT: a synchronization point between the thread filling the buffer and the writing thread where a full 
                        // buffer is exchanged for an empty one and the full buffer is written out to disk. 
                        entriesFlushing = bufferExchange.exchange(entriesFlushing); 
                        
                        writingEntry.release(group_commit_size); 
                        
                        groupCommit(entriesFlushing); 
                        flushInProgress.set(false); 
                        
                    } 
                    catch (InterruptedException ie) 
                    {
                        throw new RuntimeException("WAL writer thread interrupted while waiting for a new buffer" + ie.getStackTrace().toString());
                    }
                }
            } // WHILE
        }
    }
    
    private final HStoreSite hstore_site;
    private final HStoreConf hstore_conf;
    private final File outputFile;
    private final FileChannel fstream;
    private final int group_commit_size;
    private final FastSerializer singletonSerializer;
    private final LogEntry singletonLogEntry;
    private final AtomicBoolean swapBuffers = new AtomicBoolean(false); 
    private final AtomicBoolean flushInProgress = new AtomicBoolean(false);
    private final AtomicBoolean swapInProgress = new AtomicBoolean(false); 
    private final Semaphore writingEntry; 
    private final AtomicInteger flushReady;
    private final WriterThread flushThread;
    private final Exchanger<EntryBuffer[]> bufferExchange;
    private int commitBatchCounter = 0;
    private boolean stop = false;
    
    /**
     * The log entry buffers (one per partition) 
     */
    private EntryBuffer entries[];
    private EntryBuffer entriesFlushing[];
    
    private final ProfileMeasurement blockedTime;
    private final ProfileMeasurement writingTime;
    private final ProfileMeasurement networkTime;
    
    /**
     * Constructor
     * @param catalog_db
     * @param path
     */
    public CommandLogWriter(HStoreSite hstore_site, File outputFile) {
        this.hstore_site = hstore_site;
        this.hstore_conf = hstore_site.getHStoreConf();
        this.outputFile = outputFile;
        this.singletonSerializer = new FastSerializer(true, true);
        //this.group_commit_size = Math.max(1, hstore_conf.site.exec_command_logging_group_commit); //Group commit threshold, or 1 if group commit is turned off
        
        // hack, set arbitrarily high to avoid contention for log buffer
        this.group_commit_size = 100000; 
        
        LOG.info("group_commit_size: " + group_commit_size); 
        LOG.info("group_commit_timeout: " + hstore_conf.site.exec_command_logging_group_commit_timeout); 
        
        // Configure group commit parameters
        if(group_commit_size > 0)
        {
            
            this.writingEntry = new Semaphore(group_commit_size, false); 
            this.flushReady = new AtomicInteger(0);
            this.bufferExchange = new Exchanger<EntryBuffer[]>();
            
            // Make one entry buffer per partition SO THAT SYNCHRONIZATION ON EACH BUFFER IS NOT REQUIRED
            int num_partitions = hstore_site.getLocalPartitionIds().size();//CatalogUtil.getNumberOfPartitions(hstore_site.getDatabase());
            this.entries = new EntryBuffer[num_partitions];
            this.entriesFlushing = new EntryBuffer[num_partitions];
            for (int partition = 0; partition < num_partitions; partition++) {
                //if (hstore_site.isLocalPartition(partition)) {
                this.entries[partition] = new EntryBuffer(group_commit_size, new FastSerializer(hstore_site.getBufferPool()));
                this.entriesFlushing[partition] = new EntryBuffer(group_commit_size, new FastSerializer(hstore_site.getBufferPool()));
                //}
            } // FOR
            this.flushThread = new WriterThread();
            this.singletonLogEntry = null;
        } else {
            this.writingEntry = null; 
            this.flushReady = null;
            this.bufferExchange = null;
            this.flushThread = null;
            this.singletonLogEntry = new LogEntry();
        }
        
        FileOutputStream f = null;
        try {
            this.outputFile.getParentFile().mkdirs();
            LOG.info("Command Log File: " + this.outputFile.getParentFile().toString());
            this.outputFile.createNewFile();
            f = new FileOutputStream(this.outputFile, false);
        } catch (IOException ex) {
            throw new RuntimeException(ex);
        }
        this.fstream = f.getChannel();
        
        // Write out a header to the file 
        this.writeHeader();
        
        if (group_commit_size > 0) {
            this.flushThread.start();
        }
        
        // Writer Profiling
        if (hstore_conf.site.exec_command_logging_profile) {
            this.writingTime = new ProfileMeasurement("WRITING");
            this.blockedTime = new ProfileMeasurement("BLOCKED");
            this.networkTime = new ProfileMeasurement("NETWORK");
        } else {
            this.writingTime = null;
            this.blockedTime = null;
            this.networkTime = null;
        }
    }
    
    
    @Override
    public void prepareShutdown(boolean error) {
        this.stop = true;
        this.flushThread.interrupt();
    }
    
    //For use in test cases to make sure everything flushes
    public void finishAndPrepareShutdown() {
        this.stop = true;
        while(this.flushInProgress.get()) {} //wait until it's done running
    }
    
    @Override
    public void shutdown() {
        if (debug.get()) 
            LOG.debug("Closing WAL file");
        try {
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
    
    public ProfileMeasurement getLoggerWritingTime() {
        return this.writingTime;
    }
    
    public ProfileMeasurement getLoggerBlockedTime() {
        return this.blockedTime;
    }
    
    public ProfileMeasurement getLoggerNetworkTime() {
        return this.networkTime;
    }
    
    public boolean writeHeader() {
        if (debug.get()) LOG.debug("Writing out WAL header");
        assert(this.singletonSerializer != null);
        try {
            this.singletonSerializer.clear();
            this.singletonSerializer.writeBoolean(group_commit_size > 0);//Using group commit
            this.singletonSerializer.writeInt(hstore_site.getDatabase().getProcedures().size());
            
            for (Procedure catalog_proc : hstore_site.getDatabase().getProcedures()) {
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
     * @param eb
     */
    public void groupCommit(EntryBuffer[] eb) {
        if (hstore_conf.site.exec_command_logging_profile) this.writingTime.start();
        this.commitBatchCounter++;
        
        //Write all to a single FastSerializer buffer
        this.singletonSerializer.clear();
        int txnCounter = 0;
        for (int i = 0; i < eb.length; i++) {
            EntryBuffer buffer = eb[i];
            try {
                assert(this.singletonSerializer != null);
                int start = buffer.getStart();
                for (int j = 0, size = buffer.getSize(); j < size; j++) {
                    WriterLogEntry entry = buffer.buffer[(start + j) % buffer.buffer.length];
                    this.singletonSerializer.writeObject(entry);
                    txnCounter++;
                } // FOR
                
            } catch (Exception e) {
                String message = "Failed to serialize buffer during group commit";
                throw new ServerFaultException(message, e);
            }
        } // FOR
        
        //Compress and force out to disk
        ByteBuffer compressed;
        try {
            compressed = CompressionService.compressBufferForMessaging(this.singletonSerializer.getBBContainer().b);
        } catch (IOException e) {
            throw new RuntimeException("Failed to compress WAL buffer");
        }
        
        LOG.info(String.format("Writing out %d bytes for %d txns [batchCtr=%d]",
                               compressed.limit(), txnCounter, this.commitBatchCounter)); 
        try {
            this.fstream.write(compressed);
            this.fstream.force(true);
        } catch (IOException ex) {
            String message = "Failed to group commit for buffer";
            throw new ServerFaultException(message, ex);
        } finally {
            if (hstore_conf.site.exec_command_logging_profile) this.writingTime.stop();
        }
        
        if (hstore_conf.site.exec_command_logging_profile) 
            this.networkTime.start();
        
        // Send responses
        for (int i = 0; i < eb.length; i++) {
            EntryBuffer buffer = eb[i];
            int start = buffer.getStart();
            for (int j = 0, size = buffer.getSize(); j < size; j++) {
                WriterLogEntry entry = buffer.buffer[(start + j) % buffer.buffer.length];
                hstore_site.sendClientResponse(entry.cresponse,
                                               entry.clientCallback,
                                               entry.initiateTime,
                                               entry.restartCounter);
                                
            }
            buffer.flushCleanup();
        } // FOR
        
        if (hstore_conf.site.exec_command_logging_profile) 
            this.networkTime.stop();
    }
    
    /**
     * Write a completed transaction handle out to the WAL file
     * Returns true if the entry has been successfully written to disk and
     * the HStoreSite needs to send out the ClientResponse
     * @param ts
     * @return
     */
    public boolean appendToLog(final LocalTransaction ts, final ClientResponseImpl cresponse) {
        
        boolean sendResponse = true;
        
        if(group_commit_size > 0)
        { 
            int basePartition = ts.getBasePartition();
            assert(hstore_site.isLocalPartition(basePartition));
            basePartition = hstore_site.getLocalPartitionOffset(basePartition);
            
            // get the buffer for the partition of the current transaction
            EntryBuffer buffer = this.entries[basePartition];
            assert(buffer != null) : "Unexpected log entry buffer for partition " + basePartition;
            
            try 
            {	
                // if a swap is not yet in progress, initiate a swap with the write thread
                // this ensures exactly one thread initiates the swap with the writer thread
                if(swapBuffers.compareAndSet(true, false))
                {
                    swapInProgress.set(true); 
                    
                    // SYNC POINT: Will synchronize with writing thread
                    this.entries = this.bufferExchange.exchange(entries);
                    
                    swapInProgress.set(false); 
                }
                
                // acquire semaphore permit to write a transaction to the log buffer
                // will wait if buffer is currently being swapped
                writingEntry.acquire(); 
                
                // create an entry for this transaction in the buffer for this partition
                // NOTE: this is guaranteed to be thread-safe because there is only one thread per partition
                LogEntry entry = buffer.next(ts, cresponse);
                assert(entry != null);
                
                writingEntry.release(); 
            } 
            catch (InterruptedException e) 
            {
                throw new RuntimeException("[WAL] Thread interrupted while waiting for WriterThread to finish writing");
            } 
            finally 
            {
                if (hstore_conf.site.exec_command_logging_profile) 
                    this.blockedTime.stop();
            }
            
            // We always want to set this to false because our flush thread will be the
            // one that actually sends out the network messages
            sendResponse = false;
        } 
        else //NO GROUP COMMIT -- FINISH AND RETURN TRUE
        { 
            try 
            {
                FastSerializer fs = this.singletonSerializer;
                assert(fs != null);
                fs.clear();
                this.singletonLogEntry.init(ts);
                fs.writeObject(this.singletonLogEntry);
                BBContainer b = fs.getBBContainer();
                this.fstream.write(b.b.asReadOnlyBuffer());
                this.fstream.force(true);
                this.singletonLogEntry.finish();
            } 
            catch (Exception e) 
            {
                String message = "Failed to write single log entry for " + ts.toString();
                throw new ServerFaultException(message, e, ts.getTransactionId());
            }
        }
        
        return (sendResponse);
    }
}  


