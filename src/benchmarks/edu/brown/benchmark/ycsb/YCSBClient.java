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
import java.util.Random;

import org.apache.log4j.Logger;
import org.voltdb.CatalogContext;
import org.voltdb.client.Client;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.ProcedureCallback;

import edu.brown.api.BenchmarkComponent;
import edu.brown.benchmark.ycsb.distributions.ZipfianGenerator;
import edu.brown.logging.LoggerUtil;
import edu.brown.logging.LoggerUtil.LoggerBoolean;
import edu.brown.rand.RandomDistribution.FlatHistogram;
import edu.brown.statistics.Histogram;
import edu.brown.statistics.ObjectHistogram;

/**
 * YCSB Client
 * @author jdebrabant
 * @author pavlo
 */
public class YCSBClient extends BenchmarkComponent {
    private static final Logger LOG = Logger.getLogger(YCSBClient.class);
    private static final LoggerBoolean debug = new LoggerBoolean();
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

    private final long init_record_count;
//    private final CustomSkewGenerator readRecord;
    private final ZipfianGenerator readRecord;
    @SuppressWarnings("unused")
    private final ZipfianGenerator insertRecord;
//    private final CustomSkewGenerator insertRecord;
    private final ZipfianGenerator randScan;
    private final FlatHistogram<Transaction> txnWeights;
    private final Random rand_gen;
    private double skewFactor = YCSBConstants.ZIPFIAN_CONSTANT;
    
    int run_count = 0; 
    
    // private ZipfianGenerator readRecord;
    
    public static void main(String args[]) {
        BenchmarkComponent.main(YCSBClient.class, args, false);
    }

    public YCSBClient(String args[]) {
        super(args);

        boolean useFixedSize = false;
        long fixedSize = -1;
        for (String key : m_extraParams.keySet()) {
            String value = m_extraParams.get(key);

            // Used Fixed-size Database
            // Parameter that points to where we can find the initial data files
            if (key.equalsIgnoreCase("fixed_size")) {
                useFixedSize = Boolean.valueOf(value);
            }
            // Fixed Database Size
            else if (key.equalsIgnoreCase("num_records")) {
                fixedSize = Long.valueOf(value);
            }
            // Zipfian Skew Factor
            else if (key.equalsIgnoreCase("skew_factor")) {
                this.skewFactor = Double.valueOf(value);
            }
        } // FOR
        
        // Figure out the # of records that we need
        if (useFixedSize && fixedSize > 0) {
            this.init_record_count = fixedSize;
        }
        else {
            this.init_record_count = (long)Math.round(YCSBConstants.NUM_RECORDS * this.getScaleFactor());
        }
        this.rand_gen = new Random(); 
        this.randScan = new ZipfianGenerator(YCSBConstants.MAX_SCAN);
                
//        // initialize distribution generators 
//        // We must know where to start inserting
//        this.insertRecord = new CustomSkewGenerator(this.rand_gen, this.init_record_count, 
//                                            YCSBConstants.HOT_DATA_WORKLOAD_SKEW, YCSBConstants.HOT_DATA_SIZE, 
//                                            YCSBConstants.WARM_DATA_WORKLOAD_SKEW, YCSBConstants.WARM_DATA_SIZE);
//
//        this.readRecord = new CustomSkewGenerator(this.rand_gen, this.init_record_count, 
//                                            YCSBConstants.HOT_DATA_WORKLOAD_SKEW, YCSBConstants.HOT_DATA_SIZE, 
//                                            YCSBConstants.WARM_DATA_WORKLOAD_SKEW, YCSBConstants.WARM_DATA_SIZE);
        
        this.insertRecord = new ZipfianGenerator(this.init_record_count, this.skewFactor);
        this.readRecord = new ZipfianGenerator(this.init_record_count, this.skewFactor);
        
        // Initialize the sampling table
        Histogram<Transaction> txns = new ObjectHistogram<Transaction>(); 
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
            while (true) {
                runOnce();
                this.run_count++; 
            } 
        } 
        catch (IOException e) {
            
        }
    }
    
    @Override
    public boolean runOnce() throws IOException {
        // pick random transaction to call, weighted by txnWeights
        final Transaction target = this.txnWeights.nextValue();
        
        Object params[];
        switch (target) {
            case DELETE_RECORD:
            case READ_RECORD: {
                params = new Object[]{ this.readRecord.nextInt() };
                break;
            }
            case UPDATE_RECORD:
            case INSERT_RECORD: {
                int key = this.insertRecord.nextInt();
                String fields[] = new String[YCSBConstants.NUM_COLUMNS];
                for (int i = 0; i < fields.length; i++) {
                    fields[i] = YCSBUtil.astring(YCSBConstants.COLUMN_LENGTH, YCSBConstants.COLUMN_LENGTH);
                } // FOR
                params = new Object[]{ key, fields };
                break;
            }
            case SCAN_RECORD:
                params = new Object[]{ this.readRecord.nextInt(), this.randScan.nextInt() };
                break;
            default:
                throw new RuntimeException("Unexpected txn '" + target + "'");
        } // SWITCH
        assert(params != null);
        
        Callback callback = new Callback(target.ordinal());
        return this.getClientHandle().callProcedure(callback, target.callName, params);
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
