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

import java.io.IOException;
import java.util.HashMap;
import java.util.Random;

import org.apache.log4j.Logger;
import org.voltdb.VoltProcedure.VoltAbortException;
import org.voltdb.VoltTable;
import org.voltdb.client.Client;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.ProcedureCallback;

import edu.brown.benchmark.BenchmarkComponent;
import edu.brown.benchmark.wikipedia.procedures.GetPagesInfo;
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
	public HashMap<Long, Object[]> m_titleMap = new HashMap<Long, Object[]>();
	
	
	// TODO:(xin)
	// Change from hashMap to array in order to make it faster
	// Right now it is fine to use
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
            Object data[] = { namespace, title}; 
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
	    LOG.info("RunOnce started...");    
	    final Transaction target = this.selectTransaction();
	        
	        this.startComputeTime(target.displayName);
	        Object params[] = this.generateParams(target, user_id, page_id);
	        LOG.info("Get params for Stored procedure" + target + ", params are:" + params);
	        this.stopComputeTime(target.displayName);
	        boolean ret = this.getClientHandle().callProcedure(this.callbacks[target.ordinal()], target.callName, params);
	        
	        LOG.info("Executing txn " + target);
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
        
        Object data[] = m_titleMap.get(page_id);
        Object params[] = null;
        
        switch (txn) {
            // AddWatchList
            case ADD_WATCHLIST:    
                params = new Object[]{
                        user_id, 
                        data[0], 
                        data[1]
                };
                break;
            case REMOVE_WATCHLIST:
                params = new Object[]{
                        user_id, 
                        data[0], 
                        data[1]
                };
                break;
            case UPDATE_PAGE:
                params = new Object[]{
                        this.generateUserIP(),
                        user_id, 
                        data[0], 
                        data[1]
                };
                break;
            case GET_PAGE_ANONYMOUS:
                params = new Object[]{
                        true,
                        this.generateUserIP(),
                        user_id, 
                        data[0], 
                        data[1]
                };
                break;
            case GET_PAGE_AUTHENTICATED:
                params = new Object[]{
                        true,
                        this.generateUserIP(),
                        user_id, 
                        data[0], 
                        data[1]
                };
                break;
             default:
                 assert(false):"Should not come to this point";
                 break;
        }
        assert(params != null);
        return params;
    }
}
