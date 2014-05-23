package edu.brown.benchmark.users;
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
import edu.brown.statistics.ObjectHistogram;

public class UsersClient extends BenchmarkComponent {
		private static final Logger LOG = Logger.getLogger(UsersClient.class);
	    private static final LoggerBoolean debug = new LoggerBoolean();
	    private static final LoggerBoolean trace = new LoggerBoolean();
	    static {
	        LoggerUtil.attachObserver(LOG, debug, trace);
	    }
	    public static void main(String args[]) {
	        BenchmarkComponent.main(UsersClient.class, args, false);
	    }
	    int run_count = 0;
		private FlatHistogram<Transaction> txnWeights;
		private final Random rand_gen;
		private Zipf readRecord;
		private Zipf userRecord;
		private long usersSize;
		public UsersClient(String[] args) {
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
	        this.usersSize = Math.round(UsersConstants.USERS_SIZE * this.getScaleFactor());
	        this.userRecord = new Zipf(this.rand_gen, 0, usersSize, 1.0001);

	        this.txnWeights = new FlatHistogram<Transaction>(this.rand_gen, txns);
	    }
		  public static enum Transaction {
		  GET_USERS("Get Users", UsersConstants.FREQUENCY_GET_USERS);
		  
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

	    @Override
	    protected boolean runOnce() throws IOException {
	        // pick random transaction to call, weighted by txnWeights
	        final Transaction target = this.txnWeights.nextValue();
	        
	        Object params[];
//	        ProcedureCallback callback = null;
	        switch (target) {
	            case GET_USERS: {
	                long u_id1 = this.userRecord.nextLong();// uid
	                long u_id2 = this.usersSize - u_id1;// uid
	                params = new Object[]{ u_id1, u_id2};
	                break;
	            }
	            default:
	                throw new RuntimeException("Unexpected txn '" + target + "'");
	        } // SWITCH
	        assert(params != null);
	        Callback callback = new Callback(target.ordinal());
	        boolean val = this.getClientHandle().callProcedure(callback, target.callName, params);
	        
	        return val;
	    }
	 
	    @SuppressWarnings("unused")
	    @Deprecated
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
//
//	    	
//	        try {
//	            
//	            Random rand = new Random();
//	            int key = -1; 
//	            int scan_count; 
//	            while (true) {
//	            	System.out.println("loop print");
//	                runOnce();
//	                this.run_count++; 
//	            } 
//	        } 
//	        catch (IOException e) {
//	            
//	        }
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
	 
	 
	    @Override
	    public String[] getTransactionDisplayNames() {
	        // Return an array of transaction names
	        String procNames[] = new String[UsersProjectBuilder.PROCEDURES.length];
	        for (int i = 0; i < procNames.length; i++) {
	            procNames[i] = UsersProjectBuilder.PROCEDURES[i].getSimpleName();
	        }
	        return (procNames);
	    }
	    
	}



