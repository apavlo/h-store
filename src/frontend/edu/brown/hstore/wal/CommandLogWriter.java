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
import java.nio.channels.FileChannel;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.log4j.Logger;
import org.voltdb.ClientResponseImpl;
import org.voltdb.catalog.Procedure;
import org.voltdb.exceptions.ServerFaultException;
import org.voltdb.messaging.FastSerializer;
import org.voltdb.utils.DBBPool.BBContainer;
import org.voltdb.utils.EstTime;

import com.google.protobuf.RpcCallback;

import edu.brown.catalog.CatalogUtil;
import edu.brown.hstore.HStoreSite;
import edu.brown.hstore.HStoreThreadManager;
import edu.brown.hstore.conf.HStoreConf;
import edu.brown.hstore.dtxn.LocalTransaction;
import edu.brown.hstore.interfaces.Shutdownable;
import edu.brown.logging.LoggerUtil;
import edu.brown.logging.LoggerUtil.LoggerBoolean;

/**
 * Transaction Command Log Writer
 * @author mkirsch
 * @author pavlo
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
        protected RpcCallback<byte[]> clientCallback;
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
        /**
        * Circular Atomic Integer for EntryBuffer
        */
        protected class CircularAtomicInteger extends AtomicInteger {
            private static final long serialVersionUID = -2383758844748728140L;
            private final int limit;
            
            public CircularAtomicInteger(int lim) {
                super(0);
                limit = lim;
            }
            public int getAndIncrementCircular() { //Modified AtomicInteger.getAndIncrement() to be circular around the buffer length
                for (;;) {
                    int current = this.get();
                    int next = (current + 1) % limit;
                    if (this.compareAndSet(current, next))
                        return current;
                }
            }
        } // CLASS
        
        private final FastSerializer fs;
        private final WriterLogEntry buffer[];
        private final CircularAtomicInteger idx;
        
        
        public EntryBuffer(int size, FastSerializer serializer) {
            this.fs = serializer;
            this.buffer = new WriterLogEntry[size];
            for (int i = 0; i < size; i++) {
                this.buffer[i] = new WriterLogEntry();
            } // FOR
            this.idx = new CircularAtomicInteger(size);
        }
        public FastSerializer getSerializer() {
            return this.fs;
        }
        public LogEntry next(LocalTransaction ts, ClientResponseImpl cresponse) {
            // TODO: The internal pointer to the next element does not need to be atomic
            // But we need to think about what happens if we are about to wrap around and we
            // haven't been flushed to disk yet.
            int id = this.idx.getAndIncrementCircular();
            return this.buffer[id].init(ts, cresponse);
        }
        public boolean isFlushReady() {
            //return true;
            if (debug.get())
                LOG.debug("Checking if buffer is full: " + this.buffer[this.buffer.length - 1]);

            return (this.buffer[this.buffer.length - 1].isInitialized());
        }
        public void flushCleanup() {
            for (int i = 0; i < this.buffer.length; i++)
                this.buffer[i].finish();
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
            self.setName(HStoreThreadManager.getThreadName(hstore_site, "wal"));
            
            while (stop == false) {
//                try {
                    // TODO: Block here until somebody gives us an array of EntryBuffers to write out
                    
                    // TODO: Do the group commit!
                    
                    // TODO: Exchange the buffer that we just wrote out with a new one
                    
//                } catch (InterruptedException ex) {
//                    break;
//                }
            } // WHILE
            
            // TODO: Always make sure that flush our buffers before we finish
            
        }
    }
    
    
    final HStoreSite hstore_site;
    final HStoreConf hstore_conf;
    final File outputFile;
    final FileChannel fstream;
    final int group_commit_size;
    boolean stop = false;
    private final AtomicInteger allocated = new AtomicInteger(0);
    private final WriterThread flushThread;
    
    /**
     * The log entry buffers (one per partition) 
     */
    final EntryBuffer entries[];
    
    /**
     * Constructor
     * @param catalog_db
     * @param path
     */
    public CommandLogWriter(HStoreSite hstore_site, File outputFile) {
        this.hstore_site = hstore_site;
        this.hstore_conf = hstore_site.getHStoreConf();
        this.outputFile = outputFile;
        this.group_commit_size = Math.max(1, hstore_conf.site.exec_command_logging_group_commit); //Group commit threshold, or 1 if group commit is turned off
        
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
        
        // Make one entry buffer per partition
        int num_partitions = CatalogUtil.getNumberOfPartitions(hstore_site.getDatabase());
        this.entries = new EntryBuffer[num_partitions];
        for (int partition = 0; partition < num_partitions; partition++) {
            if (hstore_site.isLocalPartition(partition)) {
                this.entries[partition] = new EntryBuffer(group_commit_size, new FastSerializer(hstore_site.getBufferPool()));
            }
        } // FOR
        
        // Write out a header to the file 
        this.writeHeader();
        
        // Start the thread that will flush out log entries
        this.flushThread = new WriterThread(); 
        this.flushThread.start();
    }
    

    @Override
    public void prepareShutdown(boolean error) {
        this.stop = true;
        this.flushThread.interrupt();
    }

    @Override
    public void shutdown() {
        if (debug.get()) LOG.debug("Closing WAL file");
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
    
    public boolean writeHeader() {
        if (debug.get()) LOG.debug("Writing out WAL header");
        FastSerializer fs = null;
        for (int i = 0; i < this.entries.length; i++) { //Get the first available serializer
            if (this.entries[i] != null) {
                fs = this.entries[i].getSerializer();
                if (fs != null)
                    break;
            }
        }
        assert(fs != null);
        try {
            fs.clear();
            fs.writeInt(hstore_site.getDatabase().getProcedures().size());
            
            for (Procedure catalog_proc : hstore_site.getDatabase().getProcedures()) {
                int procId = catalog_proc.getId();
                fs.writeInt(procId);
                fs.writeString(catalog_proc.getName());
            } // FOR
            
            BBContainer b = fs.getBBContainer();
            fstream.write(b.b.asReadOnlyBuffer());
            fstream.force(true);
        } catch (Exception e) {
            String message = "Failed to write log headers";
            throw new ServerFaultException(message, e);
        }
        
        return (true);
    }
    
    public void groupCommit() {
        // XXX: Once we have a single thread that writes out this buffer, we won't
        // have to worry about locking here because we know that no other thread
        // will be trying to write to the log file that same time that we are.
        for (int i = 0; i < this.entries.length; i++) {
            EntryBuffer buffer = this.entries[i];
            try {
                FastSerializer fs = buffer.getSerializer();
                assert(fs != null);
                fs.clear();
                for (WriterLogEntry entry : buffer.buffer) {
                    // TODO: We don't need to check whether an entry is initialized
                    // everytime. We should maintain internal pointers to the first
                    // and last offsets in our buffer that still need to be written

                    fs.writeObject(entry);
                    BBContainer b = fs.getBBContainer();
                    fstream.write(b.b.asReadOnlyBuffer());
                }
                
                
            } catch (Exception e) {
                String message = "Failed to group commit for buffer";
                throw new ServerFaultException(message, e);
            }
        } // FOR
        
        try {
            fstream.force(true);
        } catch (IOException ex) {
            String message = "Failed to group commit for buffer";
            throw new ServerFaultException(message, ex);
        }
        
        for (int i = 0; i < this.entries.length; i++) {
            EntryBuffer buffer = this.entries[i];
            for (WriterLogEntry entry : buffer.buffer) {
                // TODO: Remove this call the same way that we do above
                if (entry.isInitialized()) {
                    hstore_site.sendClientResponse(entry.cresponse,
                                                   entry.clientCallback,
                                                   entry.initiateTime,
                                                   entry.restartCounter);
                }
            }
            buffer.flushCleanup();
        }
        
    }
    
    /**
     * Write a completed transaction handle out to the WAL file
     * Returns true if the entry has been successfully written to disk and
     * it is safe for the HStoreSite to send out the ClientResponse
     * @param ts
     * @return
     */
    public boolean appendToLog(final LocalTransaction ts, final ClientResponseImpl cresponse) {
        if (debug.get()) LOG.debug(ts + " - Writing out WAL entry for committed transaction");
        
        int basePartition = ts.getBasePartition();
        
        EntryBuffer buffer = this.entries[basePartition];
        assert(buffer != null) :
            "Unexpected log entry buffer for partition " + basePartition;

        // TODO: Need to have the ability to check how long it's been since we've flushed
        // the log to disk. If it's longer than our limit, then we'll want to do that now
        // That ensures that if there is only one client thread issuing txn requests (as is
        // often the case in testing), then it won't be blocked indefinitely.
        // Added a new HStoreConf parameter that defines the time in milliseconds
        // Use EstTime.currentTimeMillis() to get the current time
        
        // This is guaranteed to be thread-safe because there is only one thread per partition
        LogEntry entry = buffer.next(ts, cresponse);
        assert(entry != null);
        
        boolean sendResponse = true;
        try {
            if (hstore_conf.site.exec_command_logging_group_commit > 0) { //GROUP COMMIT
                // TODO: Check whether we are globally above the threshold
                if (buffer.isFlushReady()) {
                    synchronized (this) {
                        // TODO: Check whether this thread is the one that got in first and needs 
                        // to do the swap with the flush thread
                        
                        // TODO: We have to acquire the 'execLock' to check whether the flush thread
                        // has finished with the last group commit batch that we told him to write out
                        // If it's not finished, then we have to wait until it is before we can tell
                        // it to write it out again
                    
                        // TODO: Wake up the flush thread and make it write out entries to disk!
                        // If the flush thread has not finished writing out our previous group commit, 
                        // then the partition execution threads will have to block here...
                        
                    } // SYNCH
                }
                // We always want to set this to false because our flush thread will be the
                // one that actually sends out the network messages
                sendResponse = false;
                
            } else { //NO GROUP COMMIT -- FINISH AND RETURN TRUE
                FastSerializer fs = buffer.getSerializer();
                assert(fs != null);
                fs.clear();
                fs.writeObject(entry);
                BBContainer b = fs.getBBContainer();
                fstream.write(b.b.asReadOnlyBuffer());
                fstream.force(true);
            }
        } catch (Exception e) {
            String message = "Failed to write log entry for " + ts.toString();
            throw new ServerFaultException(message, e, ts.getTransactionId());
        }
        return (sendResponse);
    }
    
}    
