package edu.brown.benchmark.articles;
import java.io.IOException;
import java.util.Random;
import org.voltdb.client.*;
import edu.brown.api.BenchmarkComponent;

public class ArticlesClient extends BenchmarkComponent {
	 
	    public static void main(String args[]) {
	        BenchmarkComponent.main(ArticlesClient.class, args, false);
	    }
	 
	    public ArticlesClient(String[] args) {
	        super(args);
	        for (String key : m_extraParams.keySet()) {
	            // TODO: Retrieve extra configuration parameters
	        } // FOR
	    }
	 
	    @Override
	    public void runLoop() {
	        try {
	            Client client = this.getClientHandle();
	            Random rand = new Random();
	            while (true) {
	                // Select a random transaction to execute and generate its input parameters
	                // The procedure index (procIdx) needs to the same as the array of procedure
	                // names returned by getTransactionDisplayNames()
	                int procIdx = rand.nextInt(ArticlesProjectBuilder.PROCEDURES.length);
	                String procName = ArticlesProjectBuilder.PROCEDURES[procIdx].getSimpleName();
	                Object procParams[] = null; // TODO
	 
	                // Create a new Callback handle that will be executed when the transaction completes
	                Callback callback = new Callback(procIdx);
	 
	                // Invoke the stored procedure through the client handle. This is non-blocking
	                client.callProcedure(callback, procName, procIdx);
	 
	                // Check whether all the nodes are backed-up and this client should block
	                // before sending new requests. 
	                client.backpressureBarrier();
	            } // WHILE
	        } catch (NoConnectionsException e) {
	            // Client has no clean mechanism for terminating with the DB.
	            return;
	        } catch (InterruptedException e) {
	            e.printStackTrace();
	        } catch (IOException e) {
	            // At shutdown an IOException is thrown for every connection to
	            // the DB that is lost Ignore the exception here in order to not
	            // get spammed, but will miss lost connections at runtime
	        }
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

