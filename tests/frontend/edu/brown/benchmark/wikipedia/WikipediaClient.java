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
import org.voltdb.client.Client;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.NoConnectionsException;
import org.voltdb.client.ProcedureCallback;

import edu.brown.benchmark.BenchmarkComponent;

public class WikipediaClient extends BenchmarkComponent {
    private static final Logger LOG = Logger.getLogger(WikipediaClient.class);
	//private final TransactionGenerator<WikipediaOperation> generator;

//	final Flat usersRng;
//	final int num_users;
	private Random randGenerator = new Random();
	
//	public WikipediaClient(int id, WikipediaProjectBuilder benchmarkModule,
//	                       TransactionGenerator<WikipediaOperation> generator) {
//		super(benchmarkModule, id);
//		this.generator = generator;
//		this.num_users = (int) Math.round(WikipediaConstants.USERS * this.getWorkloadConfiguration().getScaleFactor());
//		this.usersRng = new Flat(randGenerator, 1, this.num_users);
//	}
	
	 public WikipediaClient(String[] args) {
	        super(args);
	        for (String key : m_extraParams.keySet()) {
	            // TODO: Retrieve extra configuration parameters
	        } // FOR
	 }
	
	
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
	
	private String generateUserIP() {
	    return String.format("%d.%d.%d.%d", randGenerator.nextInt(255)+1,
	                                        randGenerator.nextInt(256),
	                                        randGenerator.nextInt(256),
	                                        randGenerator.nextInt(256));
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
                int procIdx = rand.nextInt(WikipediaProjectBuilder.PROCEDURES.length);
                String procName = WikipediaProjectBuilder.PROCEDURES[procIdx].getSimpleName();
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
            incrementTransactionCounter(clientResponse,this.idx);
        }
    } // END CLASS
 
    @Override
    public String[] getTransactionDisplayNames() {
        // Return an array of transaction names
        String procNames[] = new String[WikipediaProjectBuilder.PROCEDURES.length];
        for (int i = 0; i < procNames.length; i++) {
            procNames[i] = WikipediaProjectBuilder.PROCEDURES[i].getSimpleName();
        }
        return (procNames);
    }
	
    /**
     * Set of transactions structs with their appropriate parameters
     */
//    public static enum Transaction {
//        ADD_WATCHLIST("Add watch list", WikipediaConstants.FREQUENCY_ADD_WATCHLIST, new ArgGenerator() {
//            @Override
//            public Object[] genArgs(long subscriberSize) {
//                // TODO Auto-generated method stub
//                return null;
//            }
//            
//        }),
//        GET_PAGE_ANONYMOUS("Get page anonymous", WikipediaConstants.FREQUENCY_GET_PAGE_ANONYMOUS, new ArgGenerator() {
//            
//        }),
//        GET_PAGE_AUTHENTICATED("Get page authenticated", WikipediaConstants.FREQUENCY_GET_PAGE_AUTHENTICATED, new ArgGenerator() {
//            
//        }),
//        REMOVE_WATCHLIST("Remove watchlist", WikipediaConstants.FREQUENCY_REMOVE_WATCHLIST, new ArgGenerator() {
//           
//        }),
//        UPDATE_PAGE("Update page", WikipediaConstants.FREQUENCY_UPDATE_PAGE, new ArgGenerator() {
//            
//        }),;
//	}

//    @Override
//    protected TransactionStatus executeWork(TransactionType nextTransaction) throws UserAbortException, SQLException {
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
//        if (t.userId != 0) t.userId = this.usersRng.nextInt();
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
//        return (TransactionStatus.SUCCESS);
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
//	public Article getPageAnonymous(boolean forSelect, String userIp,
//			                        int nameSpace, String pageTitle) {
//		GetPageAnonymous proc = this.getProcedure(GetPageAnonymous.class);
//        assert (proc != null);
//        return proc.run(conn, forSelect, userIp, nameSpace, pageTitle);
//	}
//
//	public Article getPageAuthenticated(boolean forSelect, String userIp, int userId,
//			                            int nameSpace, String pageTitle) {
//		GetPageAuthenticated proc = this.getProcedure(GetPageAuthenticated.class);
//        assert (proc != null);
//        return proc.run(conn, forSelect, userIp, userId, nameSpace, pageTitle);
//	}
//	
//	public void addToWatchlist(int userId, int nameSpace, String pageTitle) {
//		AddWatchList proc = this.getProcedure(AddWatchList.class);
//        assert (proc != null);
//        proc.run(conn, userId, nameSpace, pageTitle);
//	}
//
//	public void removeFromWatchlist(int userId, int nameSpace, String pageTitle) {
//		RemoveWatchList proc = this.getProcedure(RemoveWatchList.class);
//        assert (proc != null);
//        proc.run(conn, userId, nameSpace, pageTitle);
//	}
//
//	public void updatePage(String userIp, int userId, int nameSpace, String pageTitle) {
//		Article a = getPageAnonymous(false, userIp, nameSpace, pageTitle);
//		conn.commit();
//		
//		// TODO: If the Article is null, then we want to insert a new page.
//		//       But we don't support that right now.
//		if (a == null) return;
//		
//		WikipediaBenchmark b = this.getBenchmarkModule();
//		int revCommentLen = b.commentLength.nextValue().intValue();
//		String revComment = TextGenerator.randomStr(randGenerator, revCommentLen);
//		int revMinorEdit = b.minorEdit.nextValue().intValue();
//		
//		// Permute the original text of the article
//		// Important: We have to make sure that we fill in the entire array
//		char newText[] = b.generateRevisionText(a.oldText.toCharArray());
//		
//	    if (LOG.isTraceEnabled())
//	        LOG.trace("UPDATING: Page: id:"+a.pageId+" ns:"+nameSpace +" title"+ pageTitle);
//		UpdatePage proc = this.getProcedure(UpdatePage.class);
//        assert (proc != null);
//        proc.run(conn, a.textId, a.pageId, pageTitle, new String(newText),
//                       nameSpace, userId, userIp, a.userText,
//                       a.revisionId, revComment, revMinorEdit);
//	}

}
