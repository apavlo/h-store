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

import java.net.UnknownHostException;
import java.sql.SQLException;

import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.Map;
import java.util.Random;

import org.voltdb.client.Client;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.ProcedureCallback;
import org.voltdb.catalog.Catalog;
import org.voltdb.catalog.Procedure;

import edu.brown.benchmark.BenchmarkComponent;
import edu.brown.rand.AbstractRandomGenerator;
import edu.brown.rand.RandomDistribution.Flat;
import edu.brown.rand.RandomDistribution.FlatHistogram;
import edu.brown.statistics.Histogram;

import edu.brown.benchmark.wikipedia.procedures.AddWatchList;
import edu.brown.benchmark.wikipedia.procedures.GetPageAnonymous;
import edu.brown.benchmark.wikipedia.procedures.GetPageAuthenticated;
import edu.brown.benchmark.wikipedia.procedures.RemoveWatchList;
import edu.brown.benchmark.wikipedia.procedures.UpdatePage;
import edu.brown.benchmark.wikipedia.util.Article;
import edu.brown.benchmark.wikipedia.util.WikipediaOperation;
import edu.brown.benchmark.wikipedia.util.TextGenerator;
//import com.oltpbenchmark.api.Worker;  // equals to BenchmarkComponent 

public class WikipediaClient extends BenchmarkComponent {
    private static final Logger LOG = Logger.getLogger(WikipediaClient.class);
	//private final TransactionGenerator<WikipediaOperation> generator;

	final Flat usersRng;
	final int num_users;
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
	
    /**
     * Set of transactions structs with their appropriate parameters
     */
    public static enum Transaction {
        ADD_WATCHLIST("Add watch list", WikipediaConstants.FREQUENCY_ADD_WATCHLIST, new ArgGenerator() {
            public Object[] genArgs(long subscriberSize) {
                long s_id = TM1Util.getSubscriberId(subscriberSize);
                return new Object[] { TM1Util.padWithZero(s_id), // s_id
                        TM1Util.number(1, 4), // sf_type
                        8 * TM1Util.number(0, 2) // start_time
                };
            }
        }),
        GET_PAGE_ANONYMOUS("Get page anonymous", WikipediaConstants.FREQUENCY_GET_PAGE_ANONYMOUS, new ArgGenerator() {
            public Object[] genArgs(long subscriberSize) {
                long s_id = TM1Util.getSubscriberId(subscriberSize);
                return new Object[] { s_id, // s_id
                        TM1Util.number(1, 4) // ai_type
                };
            }
        }),
        GET_PAGE_AUTHENTICATED("Get page authenticated", WikipediaConstants.FREQUENCY_GET_PAGE_AUTHENTICATED, new ArgGenerator() {
            public Object[] genArgs(long subscriberSize) {
                long s_id = TM1Util.getSubscriberId(subscriberSize);
                return new Object[] { s_id, // s_id
                        TM1Util.number(1, 4), // sf_type
                        8 * TM1Util.number(0, 2), // start_time
                        TM1Util.number(1, 24) // end_time
                };
            }
        }),
        REMOVE_WATCHLIST("Remove watchlist", WikipediaConstants.FREQUENCY_REMOVE_WATCHLIST, new ArgGenerator() {
            public Object[] genArgs(long subscriberSize) {
                long s_id = TM1Util.getSubscriberId(subscriberSize);
                return new Object[] { s_id // s_id
                };
            }
        }),
        UPDATE_PAGE("Update page", WikipediaConstants.FREQUENCY_UPDATE_PAGE, new ArgGenerator() {
            public Object[] genArgs(long subscriberSize) {
                long s_id = TM1Util.getSubscriberId(subscriberSize);
                return new Object[] { TM1Util.padWithZero(s_id), // sub_nbr
                        TM1Util.number(1, 4), // sf_type
                        8 * TM1Util.number(0, 2), // start_time
                        TM1Util.number(1, 24), // end_time
                        TM1Util.padWithZero(s_id) // numberx
                };
            }
        }),;
	}

    @Override
    protected TransactionStatus executeWork(TransactionType nextTransaction) throws UserAbortException, SQLException {
        WikipediaOperation t = null;
        
        Class<? extends Procedure> procClass = nextTransaction.getProcedureClass();
        boolean needUser = (procClass.equals(AddWatchList.class) ||
                            procClass.equals(RemoveWatchList.class) ||
                            procClass.equals(GetPageAuthenticated.class));    
        while (t == null) {
            t = this.generator.nextTransaction();
            if (needUser && t.userId == 0) {
                t = null;
            }
        } // WHILE
        assert(t != null);
        if (t.userId != 0) t.userId = this.usersRng.nextInt();
        
        // AddWatchList
        if (procClass.equals(AddWatchList.class)) {
            assert(t.userId > 0);
            addToWatchlist(t.userId, t.nameSpace, t.pageTitle);
        }
        // RemoveWatchList
        else if (procClass.equals(RemoveWatchList.class)) {
            assert(t.userId > 0);
            removeFromWatchlist(t.userId, t.nameSpace, t.pageTitle);
        }
        // UpdatePage
        else if (procClass.equals(UpdatePage.class)) {
            updatePage(this.generateUserIP(), t.userId, t.nameSpace, t.pageTitle);
        }
        // GetPageAnonymous
        else if (procClass.equals(GetPageAnonymous.class)) {
            getPageAnonymous(true, this.generateUserIP(), t.nameSpace, t.pageTitle);
        }
        // GetPageAuthenticated
        else if (procClass.equals(GetPageAuthenticated.class)) {
            assert(t.userId > 0);
            getPageAuthenticated(true, this.generateUserIP(), t.userId, t.nameSpace, t.pageTitle);
        }
        
        conn.commit();
        return (TransactionStatus.SUCCESS);
    }

    @Override
    protected void runLoop() throws IOException {
        // TODO Auto-generated method stub
        
    }

    @Override
    protected String[] getTransactionDisplayNames() {
        // TODO Auto-generated method stub
        return null;
    }
    
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
