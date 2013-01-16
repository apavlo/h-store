/***************************************************************************
 *  Copyright (C) 2012 by H-Store Project                                  *
 *  Brown University                                                       *
 *  Massachusetts Institute of Technology                                  *
 *  Yale University                                                        *
 *                                                                         *
 *  Coded By:  Justin A. DeBrabant (http://www.cs.brown.edu/~debrabant/)   *                                   
 *                                                                         *
 *                                                                         *
 *  Permission is hereby granted, free of charge, to any person obtaining  *
 *  a copy of this software and associated documentation files (the        *
 *  "Software"), to deal in the Software without restriction, including    *
 *  without limitation the rights to use, copy, modify, merge, publish,    *
 *  distribute, sublicense, and/or sell copies of the Software, and to     *
 *  permit persons to whom the Software is furnished to do so, subject to  *
 *  the following conditions:                                              *
 *                                                                         *
 *  The above copyright notice and this permission notice shall be         *
 *  included in all copies or substantial portions of the Software.        *
 *                                                                         *
 *  THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,        *
 *  EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF     *
 *  MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. *
 *  IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR      *
 *  OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,  *
 *  ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR  *
 *  OTHER DEALINGS IN THE SOFTWARE.                                        *
 ***************************************************************************/
package edu.brown.benchmark.ycsb;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;

import org.apache.log4j.Logger;
import org.voltdb.CatalogContext;
import org.voltdb.client.Client;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.ProcedureCallback;

import edu.brown.api.BenchmarkComponent;
import edu.brown.benchmark.ycsb.distributions.CustomSkewGenerator;
import edu.brown.benchmark.ycsb.distributions.ZipfianGenerator;
import edu.brown.logging.LoggerUtil;
import edu.brown.logging.LoggerUtil.LoggerBoolean;
import edu.brown.rand.RandomDistribution.FlatHistogram;
import edu.brown.statistics.ObjectHistogram;

public class YCSBClient extends BenchmarkComponent {
    private static final Logger LOG = Logger.getLogger(YCSBClient.class);
    private static final LoggerBoolean debug = new LoggerBoolean(LOG.isDebugEnabled());
    static {
        LoggerUtil.attachObserver(LOG, debug);
    }
    
    
    public static enum Transaction {
        INSERT_RECORD("Insert Record", YCSBConstants.FREQUENCY_INSERT_RECORD),
        DELETE_RECORD("Delete Record", YCSBConstants.FREQUENCY_DELETE_RECORD), 
        READ_RECORD("Read Record", YCSBConstants.FREQUENCY_READ_RECORD), 
        SCAN_RECORD("Scan Record", YCSBConstants.FREQUENCY_SCAN_RECORD), 
        UPDATE_RECORD("Update Record", YCSBConstants.FREQUENCY_UPDATE_RECORD);
        
        /**
         * Constructor
         */
        private Transaction(String displayName, int weight) {
            this.displayName = displayName;
            this.callName = displayName.replace(" ", "");
            this.weight = weight;
        }
        
        public final String displayName;
        public final String callName;
        public final int weight; // probability (in terms of percentage) the transaction gets executed
    
    } // TRANSCTION ENUM

    private CustomSkewGenerator readRecord; 
    private CustomSkewGenerator insertRecord;
    private ZipfianGenerator randScan;
    private List<String> value_list; 
    private final FlatHistogram<Transaction> txnWeights;
    private Random rand_gen;
    
    // private ZipfianGenerator readRecord;
    // private final double SIGMA = 1.4; 
    
    public static void main(String args[]) {
        BenchmarkComponent.main(YCSBClient.class, args, false);
    }

    public YCSBClient(String args[]) {
        super(args);

        this.rand_gen = new Random(); 
        this.randScan = new ZipfianGenerator(YCSBConstants.MAX_SCAN);
        this.value_list = new LinkedList<String>(); 
        
        final CatalogContext catalogContext = this.getCatalogContext(); 
        final int init_record_count = (int)Math.round(YCSBConstants.NUM_RECORDS * 
                                                      catalogContext.numberOfPartitions *
                                                      this.getScaleFactor());
                
        // initialize distribution generators 
        synchronized (YCSBClient.class) { // XXX: Why do we need to lock this?
            // We must know where to start inserting
            if (insertRecord == null) {
                insertRecord = new CustomSkewGenerator(this.rand_gen, init_record_count, 
                                                    YCSBConstants.HOT_DATA_WORKLOAD_SKEW, YCSBConstants.HOT_DATA_SIZE, 
                                                    YCSBConstants.WARM_DATA_WORKLOAD_SKEW, YCSBConstants.WARM_DATA_SIZE);

            }

            if (readRecord == null) {
                readRecord = new CustomSkewGenerator(this.rand_gen, init_record_count, 
                                                    YCSBConstants.HOT_DATA_WORKLOAD_SKEW, YCSBConstants.HOT_DATA_SIZE, 
                                                    YCSBConstants.WARM_DATA_WORKLOAD_SKEW, YCSBConstants.WARM_DATA_SIZE);
            }
        }  // end SYNC
        
        // Initialize the sampling table
        ObjectHistogram<Transaction> txns = new ObjectHistogram<Transaction>(); 
        for (Transaction t : Transaction.values()) {
            Integer weight = this.getTransactionWeight(t.callName);
            if (weight == null) weight = t.weight;
            txns.put(t, weight);
        } // FOR
        assert(txns.getSampleCount() == 100) : txns;
        this.txnWeights = new FlatHistogram<Transaction>(this.rand_gen, txns);
    }

    @SuppressWarnings("unused")
    @Deprecated
    @Override
    public void runLoop() {
        try {
            Client client = this.getClientHandle();
            Random rand = new Random();
            int key = -1; 
            int scan_count; 
            
            int run_count = 0; 
            
            while (true) {
                
                runOnce(); 
            } 
        } 
        catch (IOException e) {
            
        }
    }
    
    @Override
    protected boolean runOnce() throws IOException {
        // pick random transaction to call, weighted by txnWeights
        final Transaction target = this.txnWeights.nextValue();
        
        Object params[];
        switch (target) {
            case DELETE_RECORD:
            case READ_RECORD:
            case UPDATE_RECORD:
                params = new Object[]{ readRecord.nextInt() };
                break;
            case INSERT_RECORD:
                params = new Object[]{ readRecord.nextInt() };
                @SuppressWarnings("unused")
                List<String> values = buildValues(10); // FIXME
                break;
            case SCAN_RECORD:
                params = new Object[]{ readRecord.nextInt(), randScan.nextInt() };
                break;
            default:
                throw new RuntimeException("Unexpected txn '" + target + "'");
        } // SWITCH
        assert(params != null);
        
        Callback callback = new Callback(target.ordinal());
        return this.getClientHandle().callProcedure(callback, target.callName, params);
    }
    
    private List<String> buildValues(int numVals) {
        this.value_list.clear();
        for (int i = 0; i < numVals; i++) {
            this.value_list.add(YCSBUtil.astring(YCSBConstants.COLUMN_LENGTH, YCSBConstants.COLUMN_LENGTH));
        }
        return this.value_list;
    }

    private class Callback implements ProcedureCallback {
        private final int idx;

        public Callback(int idx) {
            this.idx = idx;
        }

        @Override
        public void clientCallback(ClientResponse clientResponse) {
            // Increment the BenchmarkComponent's internal counter on the
            // number of transactions that have been completed
            incrementTransactionCounter(clientResponse, this.idx);

        }
    } // END CLASS

    @Override
    public String[] getTransactionDisplayNames() {
        // Return an array of transaction names
        String procNames[] = new String[YCSBProjectBuilder.PROCEDURES.length];
        for (int i = 0; i < procNames.length; i++) {
            procNames[i] = YCSBProjectBuilder.PROCEDURES[i].getSimpleName();
        }
        return (procNames);
    }
}
