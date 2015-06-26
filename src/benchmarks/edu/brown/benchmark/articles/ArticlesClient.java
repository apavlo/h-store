package edu.brown.benchmark.articles;
import java.io.IOException;
import java.util.Random;

import org.apache.log4j.Logger;
import org.voltdb.client.Client;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.NoConnectionsException;
import org.voltdb.client.ProcedureCallback;

import edu.brown.api.BenchmarkComponent;
import edu.brown.hstore.Hstoreservice.Status;
import edu.brown.logging.LoggerUtil;
import edu.brown.logging.LoggerUtil.LoggerBoolean;
import edu.brown.rand.RandomDistribution.FlatHistogram;
import edu.brown.rand.RandomDistribution.Zipf;
import edu.brown.statistics.Histogram;
import edu.brown.rand.RandomDistribution.PowerLaw;
import edu.brown.statistics.ObjectHistogram;

public class ArticlesClient extends BenchmarkComponent {
    private static final Logger LOG = Logger.getLogger(ArticlesClient.class);
    private static final LoggerBoolean debug = new LoggerBoolean();
    private static final LoggerBoolean trace = new LoggerBoolean();
    static {
        LoggerUtil.attachObserver(LOG, debug, trace);
    }
    
    public static enum Transaction {
        GET_ARTICLE("Get Article", ArticlesConstants.FREQUENCY_GET_ARTICLE),
        GET_COMMENTS("Get Comments", ArticlesConstants.FREQUENCY_GET_COMMENTS),
        ADD_COMMENT("Add Comment", ArticlesConstants.FREQUENCY_ADD_COMMENT),
        UPDATE_USER("Update User Info", ArticlesConstants.FREQUENCY_UPDATE_USER_INFO);
    
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
        
    int run_count = 0;
    private FlatHistogram<Transaction> txnWeights;
    private final Random rand_gen;
    private final PowerLaw readRecord;
    private final PowerLaw userRecord;
    
    public ArticlesClient(String[] args) {
        super(args);
        // Initialize the sampling table
        Histogram<Transaction> txns = new ObjectHistogram<Transaction>(); 
        for (Transaction t : Transaction.values()) {
            Integer weight = this.getTransactionWeight(t.callName);
            if (weight == null) weight = t.weight;
            txns.put(t, weight);
        } // FOR
        assert(txns.getSampleCount() == 100) : txns;
        this.rand_gen = new Random();
        long articlesSize = Math.round(ArticlesConstants.ARTICLES_SIZE * this.getScaleFactor());
        long usersSize = Math.round(ArticlesConstants.USERS_SIZE * this.getScaleFactor());

        this.readRecord = new PowerLaw(this.rand_gen, 0, articlesSize, -0.95);
        this.userRecord = new PowerLaw(this.rand_gen, 0, usersSize, -0.95);

        this.txnWeights = new FlatHistogram<Transaction>(this.rand_gen, txns);
    }

    @Override
    protected boolean runOnce() throws IOException {
        // pick random transaction to call, weighted by txnWeights
        final Transaction target = this.txnWeights.nextValue();
        
        Object params[];
        switch (target) {
            case GET_ARTICLE:
            case GET_COMMENTS: {
                //long articlesSize = Math.round(ArticlesConstants.ARTICLES_SIZE * this.getScaleFactor());
                params = new Object[]{ ((Random) this.readRecord).nextInt() };
                break;
            }
            case ADD_COMMENT: {
                int a_id = this.readRecord.nextInt();
                int u_id = this.userRecord.nextInt();// uid
                String text = ArticlesUtil.astring(100, 100);
                params = new Object[]{ a_id, u_id, text};
                break;
            }
            case UPDATE_USER:
                String lastname = ArticlesUtil.astring(100, 100);
                int u_id = this.userRecord.nextInt();
                params = new Object[]{ lastname, u_id };
                break;
            default:
                throw new RuntimeException("Unexpected txn '" + target + "'");
        } // SWITCH
        assert(params != null);

        Callback callback = new Callback(target.ordinal());
        if (debug.val) LOG.debug(target.callName+"-----------"+target.ordinal());
        boolean val = this.getClientHandle().callProcedure(callback, target.callName, params);
        
        return val;
    }
 
    @Override
    public void runLoop() {
        Client client = this.getClientHandle();
        try {
            while (true) {
                this.runOnce();
                client.backpressureBarrier();
            } // WHILE
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }
 
    private class Callback implements ProcedureCallback {            
        private final int idx;
 
        public Callback(int idx) {
            this.idx = idx;
        }
        @Override
        public void clientCallback(ClientResponse clientResponse) {
            if (clientResponse.getStatus() == Status.ABORT_UNEXPECTED) {
                LOG.error(String.format("Unexpected Error in %s: %s",
                          this.idx, clientResponse.getStatusString()),
                          clientResponse.getException());
                System.out.println(String.format("Unexpected Error in %s: %s",
                        this.idx, clientResponse.getStatusString())+ clientResponse.getException());
                
            }                
            
            // Increment the BenchmarkComponent's internal counter on the
            // number of transactions that have been completed
            incrementTransactionCounter(clientResponse, this.idx);
        }
    } // END CLASS
 
    private class GetCommentsCallback implements ProcedureCallback {            
        private Client clientHandle;
        private Object[] params;
        public GetCommentsCallback(Client clientHandle, Object[] params) {
            this.clientHandle = clientHandle;
            this.params = params;
            // TODO Auto-generated constructor stub
        }
        @Override
        public void clientCallback(ClientResponse clientResponse) {
            try {
                this.clientHandle.callProcedure(null, "GetComments", this.params);
            } catch (NoConnectionsException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            } catch (IOException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }
    } // END CLASS
 
    @Override
    public String[] getTransactionDisplayNames() {
        // Return an array of transaction names
        String procNames[] = new String[ArticlesProjectBuilder.PROCEDURES.length];
        for (int i = 0; i < procNames.length; i++) {
            procNames[i] = ArticlesProjectBuilder.PROCEDURES[i].getSimpleName();
        }
        return (procNames);
    }
}
