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

/*
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.collections15.map.ListOrderedMap;
import org.apache.log4j.Logger;
import org.voltdb.ParameterSet;
import org.voltdb.SQLStmt;
import org.voltdb.catalog.Catalog;
import org.voltdb.catalog.CatalogMap;
import org.voltdb.catalog.PlanFragment;
import org.voltdb.catalog.Procedure;
import org.voltdb.catalog.Statement;
import org.voltdb.exceptions.MispredictionException;

import edu.brown.catalog.CatalogUtil;
import edu.brown.hashing.AbstractHasher;
import edu.brown.hstore.Hstoreservice.WorkFragment;
import edu.brown.hstore.conf.HStoreConf;
import edu.brown.hstore.interfaces.Loggable;
import edu.brown.logging.LoggerUtil;
import edu.brown.logging.LoggerUtil.LoggerBoolean;
import edu.brown.plannodes.PlanNodeUtil;
import edu.brown.statistics.Histogram;
import edu.brown.utils.CollectionUtil;
import edu.brown.utils.PartitionEstimator;
import edu.brown.utils.ProfileMeasurement;
import edu.brown.utils.StringUtil;
import edu.uci.ics.jung.graph.DirectedSparseMultigraph;
*/

import java.util.List;
import java.io.*;
import java.nio.ByteBuffer;


import org.voltdb.ParameterSet;
import org.voltdb.messaging.FastDeserializer;
import org.voltdb.messaging.FastSerializable;
import org.voltdb.messaging.FastSerializer;

import edu.brown.hstore.Hstoreservice.Status;
import edu.brown.hstore.dtxn.LocalTransaction;





/**
 * @author mkirsch
 */
public class WriteAheadLogger {
    
    public static final String WAL_PATH = "/ltmp/hstore/wal.log"; //"/research/hstore/mkirsch/testwal.log";
    //"/ltmp/hstore/wal2.log";
    private static long txn_count = 0;
    
    /**
     * @param ts
     * @param st
     */
    public static boolean writeCommitted(final LocalTransaction ts) {
        txn_count++;
        //Called from HStoreSite.transactionFinish()
        Long txn_id = ts.getTransactionId();
        
        String pn = ts.getProcedureName();
        ParameterSet pp = ts.getProcedureParameters();
        
        String parameters = pp.toString();
        if (parameters.length() > 0) parameters = "params";
        long time = System.currentTimeMillis();
        

        try {
            FileWriter fstream = new FileWriter(WAL_PATH, true);
            BufferedWriter out = new BufferedWriter(fstream);
            out.write(txn_count + "><" + time + "><" + txn_id.toString() + "><" + pn + "><" + parameters + "><COMMIT\n");
            out.flush();
            out.close();
            //System.out.println("<" + time + "><" + txn_id.toString() + "><" + commit + ">");
        } catch (Exception e) {
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
    public static List<LocalTransaction> getLog() {
        List<LocalTransaction> result;
        try {
            BufferedReader in = new BufferedReader(new FileReader(WAL_PATH));
            String strLine;
            while ((strLine = in.readLine()) != null)   {
                //System.out.println(strLine);
                String[] data = strLine.split("><");
                System.out.println(data[2]);
                if (data.length == 6 && data[5].equals("COMMIT")) {
                    long txn_count = Long.parseLong(data[0]);
                    long time = Long.parseLong(data[1]);
                    long txn_id = Long.parseLong(data[2]);
                    String pn = data[3];
                    String params = data[4];
                    String status = data[5];
                    //System.out.println(txn_id);
                }
            }
            in.close();
        } catch (Exception e) {
            //e.printStackTrace();
        }
        return null;
    }
}
