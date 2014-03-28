package edu.brown.benchmark.simple;

import java.io.IOException;
import java.util.Random;

import org.apache.log4j.Logger;
import org.voltdb.CatalogContext;
import org.voltdb.client.Client;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.ProcedureCallback;

import edu.brown.api.BenchmarkComponent;
import edu.brown.logging.LoggerUtil;
import edu.brown.logging.LoggerUtil.LoggerBoolean;
import edu.brown.rand.RandomDistribution.FlatHistogram;
import edu.brown.statistics.Histogram;
import edu.brown.statistics.ObjectHistogram;

public class SimpleClient extends BenchmarkComponent {
    private static final Logger LOG = Logger.getLogger(SimpleClient.class);
    private static final LoggerBoolean debug = new LoggerBoolean(true);

    static {
        LoggerUtil.attachObserver(LOG, debug);
    }
    
    public static enum Transaction {
        GET_DATA("Read Record", 100);   // Constant
        
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
    private final FlatHistogram<Transaction> txnWeights;
    private final Random rand_gen;
    
    int run_count = 0; 
    
    public static void main(String args[]) {
        BenchmarkComponent.main(SimpleClient.class, args, false);
    }

    public SimpleClient(String args[]) {
        super(args);

        long size = -1;
        for (String key : m_extraParams.keySet()) {
            String value = m_extraParams.get(key);

            // Fixed Database Size
            if (key.equalsIgnoreCase("num_records")) {
                size = Long.valueOf(value);
            }
        } // FOR
        
        // Figure out the # of records that we need
        if (size > -1) 
            this.init_record_count = size;
        else 
            this.init_record_count = 100 ;  // Constant
        
        this.rand_gen = new Random(); 

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
            while (true) {
                runOnce();
                this.run_count++; 
            } 
        } 
        catch (IOException e) {

        }
    }

    public static final Random rand = new Random();

    // taken from tpcc.RandomGenerator
    public static long number(long minimum, long maximum) {
        assert minimum <= maximum;
        long value = Math.abs(rand.nextLong()) % (maximum - minimum + 1) + minimum;
        assert minimum <= value && value <= maximum;
        return value;
    } 

    @Override
    protected boolean runOnce() throws IOException {
        final Transaction target = this.txnWeights.nextValue();

        Object params[];
        switch (target) {
            case GET_DATA: {
                               params = new Object[]{ number(0, init_record_count) };
                               break;
            }
            default:
                           throw new RuntimeException("Unexpected txn '" + target + "'");
        } // SWITCH
        assert(params != null);

        Callback callback = new Callback(target.ordinal());

        if(debug.val)
           LOG.debug("RUN: " + target.callName + " PARAMS : "+params[0]);

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
        String procNames[] = new String[SimpleProjectBuilder.PROCEDURES.length];
        for (int i = 0; i < procNames.length; i++) {
            procNames[i] = SimpleProjectBuilder.PROCEDURES[i].getSimpleName();
        }
        return (procNames);
    }
}
