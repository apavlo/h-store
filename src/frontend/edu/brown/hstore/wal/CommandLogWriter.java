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
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.log4j.Logger;
import org.voltdb.ClientResponseImpl;
import org.voltdb.catalog.Procedure;
import org.voltdb.exceptions.ServerFaultException;
import org.voltdb.messaging.FastSerializer;
import org.voltdb.utils.DBBPool.BBContainer;

import com.google.protobuf.RpcCallback;

import edu.brown.catalog.CatalogUtil;
import edu.brown.hstore.HStoreSite;
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
        protected class CircularAtomicInteger {
            private final AtomicInteger ai;
            private final int limit;
            
            public CircularAtomicInteger(int lim) {
                limit = lim;
                ai = new AtomicInteger(0);
            }
            public int getAndIncrementCircular() { //Modified AtomicInteger.getAndIncrement() to be circular around the buffer length
                for (;;) {
                    int current = ai.get();
                    int next = (current + 1) % limit;//EntryBuffer.buffer.length;
                    if (ai.compareAndSet(current, next))
                        return current;
                }
            }
        } // CLASS
        
        private WriterLogEntry buffer[];
        private CircularAtomicInteger idx;
        
        public EntryBuffer(int size) {
            this.buffer = new WriterLogEntry[size];
            for (int i = 0; i < size; i++) {
                this.buffer[i] = new WriterLogEntry();
            } // FOR
            idx = new CircularAtomicInteger(size);
        }
        public LogEntry next(LocalTransaction ts, ClientResponseImpl cresponse) {
            WriterLogEntry e = this.buffer[this.idx.getAndIncrementCircular()];
            return e.init(ts, cresponse);
        }
        public boolean isFlushReady() {
            return (this.buffer[this.buffer.length - 1].isInitialized());
        }
        public void flushCleanup() {
            for (int i = 0; i < this.buffer.length; i++)
                this.buffer[i].finish();
        }
    } // CLASS
    
    
    final HStoreSite hstore_site;
    final HStoreConf hstore_conf;
    final File outputFile;
    final FileChannel fstream;
    final int group_commit_size;
    
    /**
     * The log entry buffers (one per partition) 
     */
    final EntryBuffer entries[];
    
    /**
     * Fast serializers (one per partition)
     */
    final FastSerializer serializers[];
    
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
            // TODO: is there a more standard way to do this?
            // TODO: update to use directory rather than files?
            this.outputFile.getParentFile().mkdirs();
            this.outputFile.createNewFile();
            f = new FileOutputStream(this.outputFile, false);
        } catch (IOException ex) {
            throw new RuntimeException(ex);
        }
        this.fstream = f.getChannel();
        
        // Make one entry buffer per partition
        int num_partitions = CatalogUtil.getNumberOfPartitions(hstore_site.getDatabase());
        this.entries = new EntryBuffer[num_partitions];
        this.serializers = new FastSerializer[num_partitions];
        for (int partition = 0; partition < num_partitions; partition++) {
            if (hstore_site.isLocalPartition(partition)) {
                this.entries[partition] = new EntryBuffer(group_commit_size);
                this.serializers[partition] = new FastSerializer(hstore_site.getBufferPool());
            }
        } // FOR
        
        // Write out a header to the file 
        this.writeHeader();
    }
    

    @Override
    public void prepareShutdown(boolean error) {
        // TODO: If we're using group commit, flush out
        // all the queued entries. We should not get any more
        // transaction entries after this point
        for (int i = 0; i < this.entries.length; i++)
          this.groupCommit(this.entries[i]);
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
        // TODO Auto-generated method stub
        return false;
    }
    
    public boolean writeHeader() {
        if (debug.get()) LOG.debug("Writing out WAL header");
        FastSerializer fs = null;
        for (int i = 0; i < this.serializers.length; i++) { //Get the first available serializer
            if (this.serializers[i] != null) {
              fs = this.serializers[i];
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
    
    public void groupCommit(EntryBuffer buffer) {
        // XXX: Once we have a single thread that writes out this buffer, we won't
        // have to worry about locking here because we know that no other thread
        // will be trying to write to the log file that same time that we are.
        synchronized (this) {
            try {
                fstream.force(true);
                            
                //TODO: NOW CALLBACK WITH ALL OF THE LOCALTRANSACTIONS
                //...
                //...
                for (WriterLogEntry entry : buffer.buffer) {
                    hstore_site.sendClientResponse(entry.cresponse,
                                                   entry.clientCallback,
                                                   entry.initiateTime,
                                                   entry.restartCounter);
                                                   
                }
            
                buffer.flushCleanup();
            } catch (Exception e) {
                String message = "Failed to group commit for buffer";
                throw new ServerFaultException(message, e);
            }
        } // SYNCH
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
        LogEntry entry = buffer.next(ts, cresponse);
        assert(entry != null);
        FastSerializer fs = this.serializers[basePartition];
        assert(fs != null);
        
        // TODO: We are going to want to use group commit to queue up
        // a bunch of entries using the buffers and then push them all out
        // when we have enough.
        if (hstore_conf.site.exec_command_logging_group_commit > 0) {
            
        }
        else {
            
        }
        
        
        // TODO: Once we have group commit, then we need a way to pass back
        // a flag to the HStoreSite from this method that tells it to not send out
        // the ClientResponse until we say it's ok. Then we need some other callback
        // where we can blast out the client responses all at once.
        
        // TODO: I think that we don't want to actually serialize the object here, because
        //        we won't be able to control whether the JVM writes out the buffer all of sudden.
        //        We should just 
        //       
        //        We can do that when we do the 
        
        synchronized (this) {
            try {
                fs.clear();
                fs.writeObject(entry);
                BBContainer b = fs.getBBContainer();
                fstream.write(b.b.asReadOnlyBuffer());
                
                if (hstore_conf.site.exec_command_logging_group_commit > 0) { //GROUP COMMIT
                    if (buffer.isFlushReady()) {
                        this.groupCommit(buffer);
                        return true;
                    } else {
                        return false;
                    }
                } else {
                  fstream.force(true);
                }
            } catch (Exception e) {
                String message = "Failed to write log entry for " + ts.toString();
                throw new ServerFaultException(message, e, ts.getTransactionId());
            }
        } // SYNCH
        
        return true;
    }
    
}    
