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
import edu.brown.benchmark.wikipedia.procedures.GetPageAnonymous;
import edu.brown.benchmark.wikipedia.procedures.GetPagesInfo;
import edu.brown.benchmark.wikipedia.util.TextGenerator;
import edu.brown.benchmark.wikipedia.util.WikipediaUtil;
import edu.brown.hstore.Hstoreservice.Status;
import edu.brown.rand.RandomDistribution.Flat;
import edu.brown.rand.RandomDistribution.FlatHistogram;
import edu.brown.rand.RandomDistribution.Zipf;
import edu.brown.statistics.Histogram;
import edu.brown.utils.StringUtil;

public class WikipediaClient extends BenchmarkComponent {
    private static final Logger LOG = Logger.getLogger(WikipediaClient.class);
	//private final TransactionGenerator<WikipediaOperation> generator;

//	final Flat usersRng;
//	final int num_users;
	private Random randGenerator = new Random();
	private int nextRevId;
	public static HashMap<Integer, Object[]> m_titleMap = new HashMap<Integer, Object[]>();
	public Flat z_users = null;
	public Zipf z_pages = null;
	
	// TODO:(xin)
	// Change from hashMap to array in order to make it faster
	// Right now it is fine to use
	public Object [] m_titleArr; 

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
            //LOG.info(clientResponse);
            
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
        
        this.nextRevId = (this.getClientId()+1) * WikipediaConstants.CLIENT_NEXT_ID_OFFSET;
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
    
    @Override
    public void startCallback() {
        Client client = this.getClientHandle();
        ClientResponse cr = null;
        try {
            cr = client.callProcedure(GetPagesInfo.class.getSimpleName());
        } catch (Exception ex) {
            throw new RuntimeException("Failed to get pages info", ex);
        }
        assert(cr != null);
        assert(cr.getStatus() == Status.OK);
        
        // Execute GetArticles stored procedure and get back the list
        // of pageIds and pageTitles.
        // Build a HashMap<Long, String[]> that gives you the pageTitle for a pageId
        // Where first element is namespace, second is title
        VoltTable res[] = cr.getResults();
        VoltTable vt = res[0];
        LOG.info("vt:\n"+ vt);
        while (vt.advanceRow()) {
            int page_id = (int) vt.getLong(0);
            int namespace = (int) vt.getLong(1);
            String title = vt.getString(2);
            Object data[] = { namespace, title};
            if (!WikipediaClient.m_titleMap.containsKey(page_id)) {
                WikipediaClient.m_titleMap.put(page_id, data);
            } else {
                assert(false):"There should not have duplicate page_ids";
            }
        } // WHILE
        double m_scalefactor = 1.0;
        int num_users = (int) Math.round(WikipediaConstants.USERS * m_scalefactor);
        //int num_pages = (int) Math.round(WikipediaConstants.PAGES * m_scalefactor);
        this.z_users = new Flat(this.randGenerator, 1, num_users);
        this.z_pages = new Zipf(this.randGenerator, 1, WikipediaClient.m_titleMap.size(), WikipediaConstants.USER_ID_SIGMA);
        assert(z_users!=null && z_pages!=null):"null users or pages";
        
        
    }
 
    /**
     * Benchmark execution loop
     */
    @Override
    public void runLoop() {
        LOG.info("Starting runLoop()");
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
        
        int user_id = this.z_users.nextInt();
        int page_id = this.z_pages.nextInt();
        Transaction target = this.selectTransaction();

        this.startComputeTime(target.displayName);
        Object params[] = null;
        try {
            params = this.generateParams(target, user_id, page_id);
        } catch (Throwable ex) {
            throw new RuntimeException("Unexpected error when generating params for " + target, ex);
        } finally {
            this.stopComputeTime(target.displayName);
        }
        assert(params != null);
        boolean ret = this.getClientHandle().callProcedure(this.callbacks[target.ordinal()],
                                                           target.callName,
                                                           params);

        //LOG.info("Executing txn:" + target.callName + ",with params:" + params);
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

    protected Object[] generateParams(Transaction txn, int user_id, int page_id) throws VoltAbortException {
        assert(WikipediaClient.m_titleMap.containsKey(page_id)):"m_titleMap should contain page_id:" + page_id;
        Object data[] = WikipediaClient.m_titleMap.get(page_id);
        assert(data != null):"data should not be null";
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
            case GET_PAGE_ANONYMOUS:
                params = new Object[]{
                        page_id,
                        true,
                        this.generateUserIP(),
                        data[0], 
                        data[1]
                };
                break;
            case GET_PAGE_AUTHENTICATED:
                params = new Object[]{
                        page_id,
                        true,
                        this.generateUserIP(),
                        user_id, 
                        data[0], 
                        data[1]
                };
                break;
            case UPDATE_PAGE:
                String user_ip = this.generateUserIP();
                int namespace = (Integer) data[0];
                String pageTitle = (String) data[1];
                params = new Object[]{
                        page_id,
                        false,
                        user_ip,
                        namespace,
                        pageTitle
                };
                //String procedureName = "GetPageAnonymous";
                //Transaction target = Transaction.getTransaction(procedureName);
                //assert(target != null):"can not find procedure: " + procedureName;
                ClientResponse cr = null;
                //LOG.info("Invoking GetPageAnonymous before executing UpdatePage");
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

                String vt_userText = vt.getString(0);
                int vt_pageId = (int) vt.getLong(1);
                String vt_oldText = vt.getString(2);
                int vt_textId = (int) vt.getLong(3);
                int vt_revisionId = (int) vt.getLong(4);
                
                assert(!vt.advanceRow()):"This assert should be false, vt has only one row";
                // Permute the original text of the article
                // Important: We have to make sure that we fill in the entire array
                char[] newText = WikipediaUtil.generateRevisionText(vt_oldText.toCharArray());
                int revCommentLen = WikipediaUtil.commentLength.nextValue().intValue();
                String revComment = TextGenerator.randomStr(new Random(), revCommentLen);
                int revMinorEdit = WikipediaUtil.minorEdit.nextValue().intValue();
                
                this.nextRevId++;
                params = new Object[]{
                        this.nextRevId,
                        vt_textId,
                        vt_pageId,
                        pageTitle,
                        namespace,
                        new String(newText),
                        user_id,
                        user_ip,
                        vt_userText,
                        vt_revisionId, 
                        revComment,
                        revMinorEdit
                };
                break;
             default:
                 assert(false):"Should not come to this point";
        }
        assert(params != null);
        
        if (LOG.isDebugEnabled())
            LOG.info(txn + " Params:\n" + StringUtil.join("\n", params));
        return params;
    }
}
