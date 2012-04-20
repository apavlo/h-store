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


import java.util.List;
import java.util.Map;
import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;


import org.voltdb.ParameterSet;
import org.voltdb.catalog.Database;
import org.voltdb.catalog.Procedure;
import org.voltdb.messaging.FastDeserializer;
import org.voltdb.messaging.FastSerializable;
import org.voltdb.messaging.FastSerializer;
import org.voltdb.utils.EstTime;

import edu.brown.hstore.Hstoreservice.Status;
import edu.brown.hstore.dtxn.LocalTransaction;

/**
 * @author mkirsch
 */
public class WriteAheadLogger {
    
    public final String WAL_PATH = "/ltmp/hstore/wal.log"; //"/research/hstore/mkirsch/testwal.log";
    //"/ltmp/hstore/wal2.log";
    private long txn_count = 0;
    
    final Database catalog_db;
    final FileChannel fstream;
    final ByteBuffer buffer;
    
    public WriteAheadLogger(Database catalog_db, String path) {
        this.catalog_db = catalog_db;
        
        FileOutputStream f = null;
        try {
            f = new FileOutputStream(new File(path), false);
        } catch (IOException ex) {
            throw new RuntimeException(ex);
        }
        this.fstream = f.getChannel();
        
        // TODO(pavlo): Use a buffer pool instead of hardcoded memory size
        byte bytes[] = new byte[102400];
        this.buffer = ByteBuffer.wrap(bytes);
        
        this.writeHeader();
    }
    
    public boolean writeHeader() {
        for (Procedure catalog_proc : catalog_db.getProcedures()) {
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
     * @param ts
     * @param st
     */
    public boolean writeCommitted(final LocalTransaction ts) {
        this.buffer.rewind();
        this.buffer.putLong(ts.getTransactionId().longValue());
        this.buffer.putLong(EstTime.currentTimeMillis());
        
        /* If we have a header with a mapping to Procedure names to ProcIds, we can do this
        int procId = ts.getProcedure().getId();
        this.buffer.putInt(procId);
        */

        // TODO: Remove this once we have the header stuff working...
        String pn = ts.getProcedureName();
        this.buffer.putInt(pn.length());
        this.buffer.put(pn.getBytes()); // TODO: Remove having to copy bytes
        
        ParameterSet pp = ts.getProcedureParameters();
        byte pp_bytes[] = null;
        try {
            pp_bytes = FastSerializer.serialize(pp); // XXX: This sucks
        } catch (IOException ex) {
            throw new RuntimeException("Failed to serialize ParameterSet for " + ts, ex);
        }
        this.buffer.put(pp_bytes);
        
        try {
            // TODO(pavlo): Figure out how to mark the position in the ByteBuffer so that
            //              we only write out the byte array up to that point instead of the whole thing
            fstream.write(this.buffer);
            fstream.force(true);
            //System.out.println("<" + time + "><" + txn_id.toString() + "><" + commit + ">");
        } catch (Exception e) {
            throw new RuntimeException(e);
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
