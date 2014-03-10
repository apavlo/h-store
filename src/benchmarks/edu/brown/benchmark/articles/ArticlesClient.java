package edu.brown.benchmark.articles;
import java.io.IOException;
import java.util.Random;

import org.voltdb.client.Client;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.NoConnectionsException;
import org.voltdb.client.ProcedureCallback;

import edu.brown.api.BenchmarkComponent;
import edu.brown.rand.RandomDistribution.FlatHistogram;
import edu.brown.rand.RandomDistribution.Zipf;
import edu.brown.statistics.Histogram;
import edu.brown.statistics.ObjectHistogram;

public class ArticlesClient extends BenchmarkComponent {
	 
	    public static void main(String args[]) {
	        BenchmarkComponent.main(ArticlesClient.class, args, false);
	    }
	    int run_count = 0;
		private FlatHistogram<Transaction> txnWeights;
		private final Random rand_gen;
		private Zipf readRecord;
		private Zipf insertRecord;
		private Zipf userRecord;
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
	        this.readRecord = new Zipf(this.rand_gen, 0,
                    ArticlesConstants.ARTICLES_SIZE, 1.0001);
	        this.insertRecord = new Zipf(this.rand_gen, 0, ArticlesConstants.ARTICLES_SIZE*ArticlesConstants.MAX_COMMENTS_PER_ARTICLE, 1.0001);
	        this.userRecord = new Zipf(this.rand_gen, 0, ArticlesConstants.USERS_SIZE, 1.0001);

	        this.txnWeights = new FlatHistogram<Transaction>(this.rand_gen, txns);
	    }
		  public static enum Transaction {
		  GET_ARTICLE("Get Article", ArticlesConstants.FREQUENCY_GET_ARTICLE),
		  ADD_COMMENT("Add Comment", ArticlesConstants.FREQUENCY_ADD_COMMENT), 
		  UPDATE_USER("Update User Info", ArticlesConstants.FREQUENCY_UPDATE_USER_INFO), 
		  GET_ARTICLES("Get Articles", ArticlesConstants.FREQUENCY_GET_ARTICLES); 

		  
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
	        System.out.println("Runnin once part 1");
	        
	        Object params[];
	        switch (target) {
	            case GET_ARTICLES:
	            case GET_ARTICLE: {
	                params = new Object[]{ ((Random) this.readRecord).nextInt() };
	                System.out.println("Runnin once get article");
	                break;
	            }
	            case ADD_COMMENT: {
	                int key = this.insertRecord.nextInt(); // cid
	                int a_id = this.readRecord.nextInt(); // aid
	                int u_id = this.userRecord.nextInt();// uid
	                System.out.println("Runnin once add comments");
	                String text = ArticlesUtil.astring(100, 100);
	                params = new Object[]{ key, a_id, u_id, text};
	                break;
	            }
	            case UPDATE_USER:
	            	String lastname = ArticlesUtil.astring(100, 100);
	            	int u_id = this.userRecord.nextInt();
	            	System.out.println("Runnin once update user");
	                params = new Object[]{ lastname, u_id };
	                break;
	            default:
	                throw new RuntimeException("Unexpected txn '" + target + "'");
	        } // SWITCH
	        assert(params != null);
	        
	        Callback callback = new Callback(target.ordinal());
	        System.out.println(target.callName);
	        System.out.println(params);
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
	            // Increment the BenchmarkComponent's internal counter on the
	            // number of transactions that have been completed
	            incrementTransactionCounter(clientResponse, this.idx);
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



