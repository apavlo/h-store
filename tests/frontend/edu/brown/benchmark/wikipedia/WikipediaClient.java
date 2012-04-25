/*******************************************************************************
 * oltpbenchmark.com
 *  
 *  Project Info:  http://oltpbenchmark.com
 *  Project Members:  	Carlo Curino <carlo.curino@gmail.com>
 * 				Evan Jones <ej@evanjones.ca>
 * 				DIFALLAH Djellel Eddine <djelleleddine.difallah@unifr.ch>
 * 				Andy Pavlo <pavlo@cs.brown.edu>
 * 				CUDRE-MAUROUX Philippe <philippe.cudre-mauroux@unifr.ch>  
 *  				Yang Zhang <yaaang@gmail.com> 
 * 
 *  This library is free software; you can redistribute it and/or modify it under the terms
 *  of the GNU General Public License as published by the Free Software Foundation;
 *  either version 3.0 of the License, or (at your option) any later version.
 * 
 *  This library is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 *  without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 *  See the GNU Lesser General Public License for more details.
 ******************************************************************************/
package edu.brown.benchmark.wikipedia;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.Random;

import org.apache.log4j.Logger;
import org.voltdb.catalog.Procedure;
import org.voltdb.client.Client;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.NoConnectionsException;
import org.voltdb.client.ProcedureCallback;

import com.oltpbenchmark.benchmarks.wikipedia.util.WikipediaOperation;

import edu.brown.benchmark.BenchmarkComponent;
import edu.brown.benchmark.tm1.TM1Client.Transaction;
import edu.brown.benchmark.wikipedia.procedures.AddWatchList;
import edu.brown.benchmark.wikipedia.procedures.GetPageAnonymous;
import edu.brown.benchmark.wikipedia.procedures.GetPageAuthenticated;
import edu.brown.benchmark.wikipedia.procedures.RemoveWatchList;
import edu.brown.benchmark.wikipedia.procedures.UpdatePage;
import edu.brown.hstore.Hstoreservice.Status;
import edu.brown.rand.RandomDistribution.FlatHistogram;
import edu.brown.statistics.Histogram;

public class WikipediaClient extends BenchmarkComponent {
    private static final Logger LOG = Logger.getLogger(WikipediaClient.class);
	//private final TransactionGenerator<WikipediaOperation> generator;

//	final Flat usersRng;
//	final int num_users;
	private Random randGenerator = new Random();
	private int nextRevId;
	
	
	
	/**
     * Each Transaction element provides an ArgGenerator to create the proper
     * arguments used to invoke the stored procedure
     */
    private static interface ArgGenerator {
        /**
         * Generate the proper arguments used to invoke the given stored
         * procedure
         * 
         * @param subscriberSize
         * @return
         */
        public Object[] genArgs(long subscriberSize);
    }
	
    /**
     * Set of transactions structs with their appropriate parameters
     */
    public static enum Transaction {
        ADD_WATCHLIST("Add watch list", WikipediaConstants.FREQUENCY_ADD_WATCHLIST, new ArgGenerator() {
            @Override
            public Object[] genArgs(long subscriberSize) {
                // I am not sure how should I use Zipf to generate the 
                // parameters that I want right now
                
                
                //addToWatchlist(t.userId, t.nameSpace, t.pageTitle);
                
                return null;
                
            }
            
        }),
        GET_PAGE_ANONYMOUS("Get page anonymous", WikipediaConstants.FREQUENCY_GET_PAGE_ANONYMOUS, new ArgGenerator() {

            @Override
            public Object[] genArgs(long subscriberSize) {
                // TODO Auto-generated method stub
                return null;
            }
            
        }),
        GET_PAGE_AUTHENTICATED("Get page authenticated", WikipediaConstants.FREQUENCY_GET_PAGE_AUTHENTICATED, new ArgGenerator() {
            @Override
            public Object[] genArgs(long subscriberSize) {
                // TODO Auto-generated method stub
                return null;
            }
        }),
        REMOVE_WATCHLIST("Remove watchlist", WikipediaConstants.FREQUENCY_REMOVE_WATCHLIST, new ArgGenerator() {
            @Override
            public Object[] genArgs(long subscriberSize) {
                // TODO Auto-generated method stub
                return null;
            }
        }),
        UPDATE_PAGE("Update page", WikipediaConstants.FREQUENCY_UPDATE_PAGE, new ArgGenerator() {

            @Override
            public Object[] genArgs(long subscriberSize) {
                // TODO Auto-generated method stub
                return null;
            }
            
        }),;



        /**
         * Constructor
         */
        private Transaction (String displayName, int weight, ArgGenerator ag) {
            this.displayName = displayName;
            this.callName = displayName.replace(" ", "");
            this.weight = weight;
            this.ag = ag;

        }
        public final String displayName;
        public final String callName;
        public final int weight; // probability (in terms of percentage) the
        // transaction gets executed
        public final ArgGenerator ag;
    }
    /**
     * Callback Class
     * @author xin
     *
     */
    
    private class WikipediaCallback implements ProcedureCallback {
        private final int idx;
 
        public WikipediaCallback(int idx) {
            super();
            this.idx = idx;
        }
        @Override
        public void clientCallback(ClientResponse clientResponse) {
            // Increment the BenchmarkComponent's internal counter on the
            // number of transactions that have been completed
            incrementTransactionCounter(clientResponse,this.idx);
        }
    } // END CLASS
    
    
    /**
     * Data Members
     */

    // Storing the ordinals of transaction per tm1 probability distribution
    private final FlatHistogram<Transaction> txnWeights;
//    private final int[] txnWeights = new int[100];

    // Callbacks
    protected final WikipediaCallback callbacks[];

    /**
     * Main method
     * 
     * @param args
     */
    public static void main(String[] args) {
        BenchmarkComponent.main(WikipediaClient.class, args, false);
    }
    
    /**
     * Constructor
     * @param args
     */
    
    public WikipediaClient ( String[] args ) {
        super(args);
        
        // Initialize the sampling table
        Histogram<Transaction> txns = new Histogram<Transaction>(); 
        for (Transaction t : Transaction.values()) {
            Integer weight = this.getTransactionWeight(t.callName);
            if (weight == null) weight = t.weight;
            txns.put(t, weight);
        } // FOR
        assert(txns.getSampleCount() == 100) : txns;
        Random rand = new Random(); // FIXME
        this.txnWeights = new FlatHistogram<Transaction>(rand, txns);
        if (LOG.isDebugEnabled())
            LOG.debug("Transaction Workload Distribution:\n" + txns);
        
     // Setup callbacks
        int num_txns = Transaction.values().length;
        this.callbacks = new WikipediaCallback[num_txns];
        for (int i = 0; i < num_txns; i++) {
            this.callbacks[i] = new WikipediaCallback(i);
        } // FOR
        
        this.nextRevId = this.getClientId() * WikipediaConstants.CLIENT_NEXT_ID_OFFSET;
    }
    
    /**
     * Return a transaction randomly selected per TM1 probability specs
     */
    private Transaction selectTransaction() {
        // Transaction force = null; // (this.getClientId() == 0 ?
        // Transaction.INSERT_CALL_FORWARDING :
        // Transaction.GET_SUBSCRIBER_DATA); //
        // Transaction.INSERT_CALL_FORWARDING;
        // if (force != null) return (force);
        return this.txnWeights.nextValue();
    }
 
    /**
     * Benchmark execution loop
     */
    @Override
    public void runLoop() {
        LOG.debug("Starting runLoop()");
        final Client client = this.getClientHandle();
        
        // Execute GetArticles stored procedure and get back the list
        // of pageIds and pageTitles.
        // Build a HashMap<Long, Title> that gives you the pageTitle for a pageId

        try {
            while (true) {
                this.runOnce();
                client.backpressureBarrier();
            } // WHILE
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }
	
	 @Override
	    protected boolean runOnce() throws IOException {
	        final Transaction target = this.selectTransaction();

	        this.startComputeTime(target.displayName);
	        Object params[] = target.ag.genArgs(10);
	        this.stopComputeTime(target.displayName);

	        boolean ret = this.getClientHandle().callProcedure(this.callbacks[target.ordinal()], target.callName, params);
	        LOG.debug("Executing txn " + target);
	        return (ret);
	    }
 
	private String generateUserIP() {
        return String.format("%d.%d.%d.%d", randGenerator.nextInt(255)+1,
                                            randGenerator.nextInt(256),
                                            randGenerator.nextInt(256),
                                            randGenerator.nextInt(256));
    }
    
   
    @Override
    public String[] getTransactionDisplayNames() {
        // Return an array of transaction names
        String procNames[] = new String[WikipediaProjectBuilder.PROCEDURES.length];
        for (int i = 0; i < procNames.length; i++) {
            procNames[i] = WikipediaProjectBuilder.PROCEDURES[i].getSimpleName();
        }
        return (procNames);
    }
	
   

//    @Override
//    protected Status executeWork(TransactionType nextTransaction) throws VoltAbortException {
//        WikipediaOperation t = null;
//        
//        Class<? extends Procedure> procClass = nextTransaction.getProcedureClass();
//        boolean needUser = (procClass.equals(AddWatchList.class) ||
//                            procClass.equals(RemoveWatchList.class) ||
//                            procClass.equals(GetPageAuthenticated.class));    
//        while (t == null) {
//            t = this.generator.nextTransaction();
//            if (needUser && t.userId == 0) {
//                t = null;
//            }
//        } // WHILE
//        assert(t != null);
//        if (t.userId != 0) t.userId = this.randGenerator.nextInt();
//        
//        // AddWatchList
//        if (procClass.equals(AddWatchList.class)) {
//            assert(t.userId > 0);
//            addToWatchlist(t.userId, t.nameSpace, t.pageTitle);
//        }
//        // RemoveWatchList
//        else if (procClass.equals(RemoveWatchList.class)) {
//            assert(t.userId > 0);
//            removeFromWatchlist(t.userId, t.nameSpace, t.pageTitle);
//        }
//        // UpdatePage
//        else if (procClass.equals(UpdatePage.class)) {
//            updatePage(this.generateUserIP(), t.userId, t.nameSpace, t.pageTitle);
//        }
//        // GetPageAnonymous
//        else if (procClass.equals(GetPageAnonymous.class)) {
//            getPageAnonymous(true, this.generateUserIP(), t.nameSpace, t.pageTitle);
//        }
//        // GetPageAuthenticated
//        else if (procClass.equals(GetPageAuthenticated.class)) {
//            assert(t.userId > 0);
//            getPageAuthenticated(true, this.generateUserIP(), t.userId, t.nameSpace, t.pageTitle);
//        }
//        
//        return (Status.OK);
//    }
    
    
    /**
	 * Implements wikipedia selection of last version of an article (with and
	 * without the user being logged in)
	 * 
	 * @parama userIp contains the user's IP address in dotted quad form for
	 *         IP-based access control
	 * @param userId
	 *            the logged in user's identifer. If negative, it is an
	 *            anonymous access.
	 * @param nameSpace
	 * @param pageTitle
	 * @return article (return a Class containing the information we extracted,
	 *         useful for the updatePage transaction)
	 * @throws SQLException
	 * @throws UnknownHostException
	 */
//	public VoltTable getPageAnonymous(boolean forSelect, String userIp,
//			                        int nameSpace, String pageTitle) {
//		GetPageAnonymous proc = this.getProcedure(GetPageAnonymous.class);
//        assert (proc != null);
//        return proc.run(forSelect, userIp, nameSpace, pageTitle);
//	}
//
//	public VoltTable getPageAuthenticated(boolean forSelect, String userIp, int userId,
//			                            int nameSpace, String pageTitle) {
//		GetPageAuthenticated proc = this.getProcedure(GetPageAuthenticated.class);
//        assert (proc != null);
//        return proc.run( forSelect, userIp, userId, nameSpace, pageTitle);
//	}
//	
//	public void addToWatchlist(int userId, int nameSpace, String pageTitle) {
//		AddWatchList proc = this.getProcedure(AddWatchList.class);
//        assert (proc != null);
//        proc.run( userId, nameSpace, pageTitle);
//	}
//
//	public void removeFromWatchlist(int userId, int nameSpace, String pageTitle) {
//		RemoveWatchList proc = this.getProcedure(RemoveWatchList.class);
//        assert (proc != null);
//        proc.run( userId, nameSpace, pageTitle);
//	}
//
//	public void updatePage(String userIp, int userId, int nameSpace, String pageTitle) {
//	    VoltTable a = getPageAnonymous(false, userIp, nameSpace, pageTitle);
//		
//		// TODO: If the Article is null, then we want to insert a new page.
//		//       But we don't support that right now.
//		if (a == null) return;
//		
////		WikipediaBenchmark b = this.getBenchmarkModule();
////		int revCommentLen = b.commentLength.nextValue().intValue();
////		String revComment = TextGenerator.randomStr(randGenerator, revCommentLen);
////		int revMinorEdit = b.minorEdit.nextValue().intValue();
////		
////		// Permute the original text of the article
////		// Important: We have to make sure that we fill in the entire array
////		char newText[] = b.generateRevisionText(a.oldText.toCharArray());
//
//		UpdatePage proc = this.getProcedure(UpdatePage.class);
//        assert (proc != null);
////        proc.run(this.nextRevId++, a.pageId, pageTitle, new String(newText),
////                       nameSpace, userId, userIp, a.userText,
////                       a.revisionId, revComment, revMinorEdit);
//	}
//
}
