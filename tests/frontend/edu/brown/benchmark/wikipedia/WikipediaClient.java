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
import java.util.HashMap;
import java.util.Random;

import org.apache.log4j.Logger;
import org.voltdb.VoltProcedure;
import org.voltdb.VoltProcedure.VoltAbortException;
import org.voltdb.VoltTable;
import org.voltdb.catalog.Procedure;
import org.voltdb.client.Client;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.NoConnectionsException;
import org.voltdb.client.ProcedureCallback;

import com.oltpbenchmark.benchmarks.wikipedia.util.WikipediaOperation;

import edu.brown.benchmark.BenchmarkComponent;
import edu.brown.benchmark.wikipedia.procedures.AddWatchList;
import edu.brown.benchmark.wikipedia.procedures.GetPageAnonymous;
import edu.brown.benchmark.wikipedia.procedures.GetPageAuthenticated;
import edu.brown.benchmark.wikipedia.procedures.GetPagesInfo;
import edu.brown.benchmark.wikipedia.procedures.RemoveWatchList;
import edu.brown.benchmark.wikipedia.procedures.UpdatePage;
import edu.brown.hstore.Hstoreservice.Status;
import edu.brown.rand.RandomDistribution.Flat;
import edu.brown.rand.RandomDistribution.FlatHistogram;
import edu.brown.rand.RandomDistribution.Zipf;
import edu.brown.statistics.Histogram;

public class WikipediaClient extends BenchmarkComponent {
    private static final Logger LOG = Logger.getLogger(WikipediaClient.class);
	//private final TransactionGenerator<WikipediaOperation> generator;

//	final Flat usersRng;
//	final int num_users;
	private Random randGenerator = new Random();
	private int nextRevId;
	public HashMap<Long, String[]> m_titleMap = new HashMap<Long, String[]>();
	
	
	// TODO:(xin)
	// Change from hashMap to array in order to make it faster
	public Object [] m_titleArr; 
	
	
	 

    /**
     * Set of transactions structs with their appropriate parameters
     */
    public static enum Transaction {
        ADD_WATCHLIST("Add watch list", WikipediaConstants.FREQUENCY_ADD_WATCHLIST),
        GET_PAGE_ANONYMOUS("Get page anonymous", WikipediaConstants.FREQUENCY_GET_PAGE_ANONYMOUS),
        GET_PAGE_AUTHENTICATED("Get page authenticated", WikipediaConstants.FREQUENCY_GET_PAGE_AUTHENTICATED),
        REMOVE_WATCHLIST("Remove watchlist", WikipediaConstants.FREQUENCY_REMOVE_WATCHLIST),
        UPDATE_PAGE("Update page", WikipediaConstants.FREQUENCY_UPDATE_PAGE);

        /**
         * Constructor
         */
        private Transaction (String displayName, int weight) {
            this.displayName = displayName;
            this.callName = displayName.replace(" ", "");
            this.weight = weight;
        }
        public final String displayName;
        public final String callName;
        public final int weight; // probability (in terms of percentage) the
        // transaction gets executed
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

    // Storing the ordinals of transaction per wikipedia probability distribution
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
        
        ClientResponse cr = null;
        try {
            cr = client.callProcedure(GetPagesInfo.class.getSimpleName());

        } catch (Exception ex) {
            throw new RuntimeException("Failed to update users and pages", ex);
        }
        assert(cr != null);
        assert(cr.getStatus() == Status.OK);
        
        
        // TODO(xin):
        // Execute GetArticles stored procedure and get back the list
        // of pageIds and pageTitles.
        // Build a HashMap<Long, String[]> that gives you the pageTitle for a pageId
        // Where first element is namespace, second is title
        VoltTable res[] = cr.getResults();
        VoltTable vt = res[0];
        
        while (vt.advanceRow()) {
            long page_id = vt.getLong(0);
            long namespace = vt.getLong(1);
            String title = vt.getString(2);
            String data[] = { Long.toString(namespace), title}; 
            if (!m_titleMap.containsKey(page_id)) {
                m_titleMap.put(page_id, data);
            } else {
                assert(false):"There should not have duplicate page_ids";
            }
        }
        
        
        double m_scalefactor = 1.0;
        int num_users = (int) Math.round(WikipediaConstants.USERS * m_scalefactor);
        //int num_pages = (int) Math.round(WikipediaConstants.PAGES * m_scalefactor);
        
        Flat z_users = new Flat(this.randGenerator, 1, num_users);
        Zipf z_pages = new Zipf(this.randGenerator, 1, m_titleMap.size(), WikipediaConstants.USER_ID_SIGMA);
        
        
        try {
            while (true) {
                // Figure out what page they're going to update
                int user_id = z_users.nextInt();
                int page_id = z_pages.nextInt();
                
                this.runOnce(user_id,page_id);
                client.backpressureBarrier();
            } // WHILE
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }
	
	 
	    protected boolean runOnce(int user_id,int page_id) throws IOException {
	        final Transaction target = this.selectTransaction();

	        this.startComputeTime(target.displayName);
	        Object params[] = this.generateParams(target, user_id, page_id);
	       
	        this.stopComputeTime(target.displayName);
	        boolean ret = this.getClientHandle().callProcedure(this.callbacks[target.ordinal()], target.callName, params);
	        
	        LOG.debug("Executing txn " + target);
	        return ret;
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
	

    protected Object[] generateParams(Transaction txn, int user_id, int page_id) throws VoltAbortException {
        
        String data[] = m_titleMap.get(page_id);
       
        // AddWatchList
        if (txn.callName.equals(AddWatchList.class.getName())) {
            
            Object params[] = {
                    user_id, 
                    new Integer(data[0]), 
                    data[1]
            };
            return params;
        }
        // RemoveWatchList
        else if (txn.callName.equals(RemoveWatchList.class.getName())) {
            Object params[] = {
                    user_id, 
                    new Integer(data[0]), 
                    data[1]
            };
            return params;
        }
        // UpdatePage
        else if (txn.callName.equals(UpdatePage.class.getName())) {
            Object params[] = {
                    this.generateUserIP(),
                    user_id, 
                    new Integer(data[0]), 
                    data[1]
            };
            return params;
        }
        // GetPageAnonymous
        else if (txn.callName.equals(GetPageAnonymous.class.getName())) {
            Object params[] = {
                    true,
                    this.generateUserIP(),
                    user_id, 
                    new Integer(data[0]), 
                    data[1]
            };
            return params;
        }
        // GetPageAuthenticated
        else if (txn.callName.equals(GetPageAuthenticated.class.getName())) {
            Object params[] = {
                    true,
                    this.generateUserIP(),
                    user_id, 
                    new Integer(data[0]), 
                    data[1]
            };
            return params;
        }
        
        assert(false):"Should never come to this point";
        return null;
    }
    
    
//    /**
//	 * Implements wikipedia selection of last version of an article (with and
//	 * without the user being logged in)
//	 * 
//	 * @parama userIp contains the user's IP address in dotted quad form for
//	 *         IP-based access control
//	 * @param userId
//	 *            the logged in user's identifer. If negative, it is an
//	 *            anonymous access.
//	 * @param nameSpace
//	 * @param pageTitle
//	 * @return article (return a Class containing the information we extracted,
//	 *         useful for the updatePage transaction)
//	 * @throws SQLException
//	 * @throws UnknownHostException
//	 */
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

}
