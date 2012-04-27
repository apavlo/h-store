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
import java.util.Map;

import org.apache.log4j.Logger;
import org.voltdb.catalog.Procedure;
import org.voltdb.exceptions.ServerFaultException;
import org.voltdb.messaging.FastDeserializer;
import org.voltdb.messaging.FastSerializable;
import org.voltdb.messaging.FastSerializer;
import org.voltdb.utils.DBBPool.BBContainer;
import org.voltdb.utils.EstTime;

import edu.brown.catalog.CatalogUtil;
import edu.brown.hstore.HStoreSite;
import edu.brown.hstore.conf.HStoreConf;
import edu.brown.hstore.dtxn.LocalTransaction;
import edu.brown.hstore.interfaces.Shutdownable;
import edu.brown.logging.LoggerUtil;
import edu.brown.logging.LoggerUtil.LoggerBoolean;

/**
 * Transaction Command Logger
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
    
    public final String WAL_PATH = "/ltmp/hstore/wal.log"; //"/research/hstore/mkirsch/testwal.log";
    //"/ltmp/hstore/wal2.log";
    private long txn_count = 0;
    
    /**
     * Circular Buffer of Log Entries
     */
    protected class EntryBuffer {
        private LogEntry buffer[];
        private int idx;
        
        public EntryBuffer(int size) {
            this.buffer = new LogEntry[size];
            for (int i = 0; i < size; i++) {
                this.buffer[i] = new LogEntry();
            } // FOR
        }
        public LogEntry next(LocalTransaction ts) {
            if (this.idx == this.buffer.length) {
                this.idx = 0;
            }
            LogEntry e = this.buffer[this.idx++];
            e.txnId = ts.getTransactionId();
            e.procName = ts.getProcedureName();
            e.procParams = ts.getProcedureParameters();
            e.flushed = false;
            return (e);
        }
    } // CLASS
    
    
    final HStoreSite hstore_site;
    final HStoreConf hstore_conf;
    final FileChannel fstream;
    
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
    public CommandLogWriter(HStoreSite hstore_site, String path) {
        this.hstore_site = hstore_site;
        this.hstore_conf = hstore_site.getHStoreConf();
        
        FileOutputStream f = null;
        try {
            f = new FileOutputStream(new File(path), false);
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
                this.entries[partition] = new EntryBuffer(50); // XXX
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
        for (Procedure catalog_proc : hstore_site.getDatabase().getProcedures()) {
            int procId = catalog_proc.getId();
            
            // TODO: Write out a header that contains a mapping from Procedure name to ids
        } // FOR
        return (true);
    }
    
    /**
     * 
     * @return
     */
    public Map<Integer, String> readHeader() {
        
        return (null);
    }

    /**
     * Write a completed transaction handle out to the WAL file
     * Returns true if the entry has been successfully written to disk and
     * it is safe for the HStoreSite to send out the ClientResponse
     * @param ts
     * @return
     */
    public boolean write(final LocalTransaction ts) {
        if (debug.get()) LOG.debug(ts + " - Writing out WAL entry for committed transaction");
        
        int basePartition = ts.getBasePartition();
        EntryBuffer buffer = this.entries[basePartition];
        assert(buffer != null) :
            "Unexpected log entry buffer for partition " + basePartition;
        LogEntry entry = buffer.next(ts);
        assert(entry != null);
        FastSerializer fs = this.serializers[basePartition];
        assert(fs != null);
        
        // TODO: We are going to want to use group commit to queue up
        // a bunch of entries using the buffers and then push them all out
        // when we have enough.
        // TODO: Once we have group commit, then we need a way to pass back
        // a flag to the HStoreSite from this method that tells it to not send out
        // the ClientResponse until we say it's ok. Then we need some other callback
        // where we can blast out the client responses all at once.
        
        try {
            fs.clear();
            BBContainer b = fs.writeObjectForMessaging(entry);
            fstream.write(b.b.asReadOnlyBuffer());
            
            // TODO: We should have an asynchronous option here like postgres
            // where we don't have to wait until the OS flushes the changes out
            // to the file before we're allowed to continue.
            fstream.force(true);
            entry.flushed = true;
            //System.out.println("<" + time + "><" + txn_id.toString() + "><" + commit + ">");
        } catch (Exception e) {
            String message = "Failed to write log entry for " + ts.toString();
            throw new ServerFaultException(message, e, ts.getTransactionId());
        }
        
        return true;
    }



    /**
     * @param cp
     */
    /*public static boolean applyCheckpoint(final Checkpoint cp) {
        
        return true;
    }*/
    
    /**
     * 
     */
//    public static List<LocalTransaction> getLog() {
//        List<LocalTransaction> result;
//        try {
//            BufferedReader in = new BufferedReader(new FileReader(WAL_PATH));
//            String strLine;
//            while ((strLine = in.readLine()) != null)   {
//                //System.out.println(strLine);
//                String[] data = strLine.split("><");
//                System.out.println(data[2]);
//                if (data.length == 6 && data[5].equals("COMMIT")) {
//                    long txn_count = Long.parseLong(data[0]);
//                    long time = Long.parseLong(data[1]);
//                    long txn_id = Long.parseLong(data[2]);
//                    String pn = data[3];
//                    String params = data[4];
//                    String status = data[5];
//                    //System.out.println(txn_id);
//                }
//            }
//            in.close();
//        } catch (Exception e) {
//            //e.printStackTrace();
//        }
//        return null;
//    }
}
