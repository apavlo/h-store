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
import java.util.Random;

import org.apache.log4j.Logger;
import org.voltdb.VoltProcedure.VoltAbortException;
import org.voltdb.VoltTable;
import org.voltdb.client.Client;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.ProcedureCallback;

import edu.brown.api.BenchmarkComponent;
import edu.brown.benchmark.wikipedia.procedures.GetPageAnonymous;
import edu.brown.benchmark.wikipedia.util.TextGenerator;
import edu.brown.benchmark.wikipedia.util.WikipediaUtil;
import edu.brown.logging.LoggerUtil;
import edu.brown.logging.LoggerUtil.LoggerBoolean;
import edu.brown.rand.RandomDistribution.Flat;
import edu.brown.rand.RandomDistribution.FlatHistogram;
import edu.brown.rand.RandomDistribution.Zipf;
import edu.brown.statistics.ObjectHistogram;
import edu.brown.utils.StringUtil;

public class WikipediaClient extends BenchmarkComponent {
    private static final Logger LOG = Logger.getLogger(WikipediaClient.class);
    private static final LoggerBoolean debug = new LoggerBoolean();
    private static final LoggerBoolean trace = new LoggerBoolean();
    static {
        LoggerUtil.attachObserver(LOG, debug, trace);
    }

	private Random randGenerator = new Random();
	private final WikipediaUtil util;

	
	private final Flat flat_users;
	private final Zipf zipf_pages;
	private final Flat flat_pages;
	
    /**
     * Set of transactions structs with their appropriate parameters
     */
    public static enum Transaction {
        ADD_WATCHLIST("Add WatchList", WikipediaConstants.FREQUENCY_ADD_WATCHLIST),
        GET_PAGE_ANONYMOUS("Get Page Anonymous", WikipediaConstants.FREQUENCY_GET_PAGE_ANONYMOUS),
        GET_PAGE_AUTHENTICATED("Get Page Authenticated", WikipediaConstants.FREQUENCY_GET_PAGE_AUTHENTICATED),
        REMOVE_WATCHLIST("Remove Watch List", WikipediaConstants.FREQUENCY_REMOVE_WATCHLIST),
        UPDATE_PAGE("Update Page", WikipediaConstants.FREQUENCY_UPDATE_PAGE);

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
        
        public static Transaction getTransaction (String callname) {
            Transaction res = null;
            for (Transaction t : Transaction.values()) {
                if (callname.equals(t.callName))
                    res = t;
            }
            return res;
        }
        
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
            //if (debug.val) LOG.debug(clientResponse);
            
            // Increment the BenchmarkComponent's internal counter on the
            // number of transactions that have been completed
            incrementTransactionCounter(clientResponse, this.idx);
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
    
    public WikipediaClient(String[] args) {
        super(args);
        
        // Initialize the sampling table
        ObjectHistogram<Transaction> txns = new ObjectHistogram<Transaction>(); 
        for (Transaction t : Transaction.values()) {
            Integer weight = this.getTransactionWeight(t.callName);
            if (weight == null) weight = t.weight;
            txns.put(t, weight);
        } // FOR
        assert(txns.getSampleCount() == 100) : txns;
        this.txnWeights = new FlatHistogram<Transaction>(this.randGenerator, txns);
        if (debug.val) LOG.debug("Transaction Workload Distribution:\n" + txns);
        
        // Setup callbacks
        int num_txns = Transaction.values().length;
        this.callbacks = new WikipediaCallback[num_txns];
        for (int i = 0; i < num_txns; i++) {
            this.callbacks[i] = new WikipediaCallback(i);
        } // FOR
        
        this.util = new WikipediaUtil(this.randGenerator, this.getScaleFactor());
        
        this.flat_users = new Flat(this.randGenerator, 1, util.num_users);
        this.zipf_pages = new Zipf(this.randGenerator, 1, util.num_pages, WikipediaConstants.USER_ID_SIGMA);
        this.flat_pages = new Flat(this.randGenerator, 1, util.num_pages);
    }
    
    /**
     * Return a transaction randomly selected per WikipediaClient probability specs
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
        if (debug.val) LOG.debug("Starting runLoop()");
        Client client = this.getClientHandle();
        try {
            while (true) {
                // Figure out what page they're going to update
                this.runOnce();
                client.backpressureBarrier();
            } // WHILE
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }
    
    @Override
	protected boolean runOnce() throws IOException {
        Transaction target = this.selectTransaction();
        this.startComputeTime(target.displayName);
        Object params[] = null;
        try {
            params = this.generateParams(target);
        } catch (Throwable ex) {
            throw new RuntimeException("Unexpected error when generating params for " + target, ex);
        } finally {
            this.stopComputeTime(target.displayName);
        }
        assert(params != null);
        boolean ret = this.getClientHandle().callProcedure(this.callbacks[target.ordinal()],
                                                           target.callName,
                                                           params);

        //if (debug.val) LOG.debug("Executing txn:" + target.callName + ",with params:" + params);
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
        String procNames[] = new String[Transaction.values().length];
        for (int i = 0; i < procNames.length; i++) {
            procNames[i] = Transaction.values()[i].displayName;
        }
        return (procNames);
    }

    protected Object[] generateParams(Transaction txn) {
        Object params[] = null;
        switch (txn) {
            case ADD_WATCHLIST:
            case REMOVE_WATCHLIST: {
                long pageId = this.flat_pages.nextLong();
                int nameSpace = util.getPageNameSpace(pageId);
                int userId = this.flat_users.nextInt();
                params = new Object[]{
                        userId, 
                        nameSpace, 
                        pageId
                };
                break;
            }
            case GET_PAGE_ANONYMOUS: {
                String userIp = this.generateUserIP();
                long pageId = this.zipf_pages.nextLong();
                int nameSpace = util.getPageNameSpace(pageId);
                params = new Object[]{
                        pageId,
                        nameSpace,
                        userIp,
                        true,
                };
                break;
            }
            case GET_PAGE_AUTHENTICATED: {
                int userId = this.flat_users.nextInt();
                String userIp = this.generateUserIP();
                long pageId = this.zipf_pages.nextLong();
                int nameSpace = util.getPageNameSpace(pageId);
                params = new Object[]{
                        pageId,
                        nameSpace,
                        userId, 
                        userIp,
                        true,
                };
                break;
            }
            case UPDATE_PAGE: {
                int userId = this.flat_users.nextInt();
                long pageId = this.zipf_pages.nextLong();
                int nameSpace = util.getPageNameSpace(pageId);
                String user_ip = this.generateUserIP();
                params = new Object[]{
                        pageId,
                        nameSpace,
                        user_ip,
                        false,
                };
                ClientResponse cr = null;
                if (debug.val) LOG.debug("Invoking GetPageAnonymous before executing UpdatePage");
                try {
                    cr = this.getClientHandle().callProcedure(GetPageAnonymous.class.getSimpleName(), params);
                } catch (Exception e) {
                    throw new RuntimeException("Failed to call GetPageAnonymous...", e);
                }
                assert(cr != null);
                if (LOG.isDebugEnabled()) 
                    LOG.debug("GetPageAnonymous Result:\n" + cr);
                
                // voltTable
                VoltTable res[] = cr.getResults();
                VoltTable vt = res[0];
                
                // deal with the VoltTable and try to ...
                if (!vt.advanceRow() ) {
                    String msg = "No result for the GetPageAnonymous stored procedure";
                    throw new VoltAbortException(msg);
                }

                String vt_userText = vt.getString("USER_TEXT");
                long vt_pageId = vt.getLong("PAGE_ID");
                String vt_pageTitle = vt.getString("PAGE_TITLE");
                String vt_oldText = vt.getString("OLD_TEXT");
                int vt_textId = (int) vt.getLong("TEXT_ID");
                int vt_revisionId = (int) vt.getLong("REVISION_ID");

                assert(vt_pageId == pageId) : String.format("pageId=%d / pageTitle=%d", vt_pageId, vt_pageTitle);
                assert(vt.advanceRow() == false) : "This assert should be false, vt has only one row";

                // Permute the original text of the article
                // Important: We have to make sure that we fill in the entire array
                String newText = new String(util.generateRevisionText(vt_oldText.toCharArray()));
                int revCommentLen = util.h_commentLength.nextValue().intValue();
                String revComment = TextGenerator.randomStr(this.randGenerator, revCommentLen);
                int revMinorEdit = util.h_minorEdit.nextValue().intValue();
                
                params = new Object[]{
                        vt_pageId,
                        vt_textId,
                        nameSpace,
                        newText,
                        userId,
                        user_ip,
                        vt_userText,
                        vt_revisionId, 
                        revComment,
                        revMinorEdit
                };
                break;
            }
            default:
                 assert(false):"Should not come to this point";
        }
        assert(params != null);
        
        if (debug.val) LOG.debug(txn + " Params:\n" + StringUtil.join("\n", params));
        return params;
    }
}
