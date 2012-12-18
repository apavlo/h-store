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

import org.voltdb.VoltProcedure;

import edu.brown.benchmark.AbstractProjectBuilder;
import edu.brown.api.BenchmarkComponent;
import edu.brown.benchmark.wikipedia.procedures.GetTableCounts;
import edu.brown.benchmark.wikipedia.procedures.AddWatchList;
import edu.brown.benchmark.wikipedia.procedures.GetPageAnonymous;
import edu.brown.benchmark.wikipedia.procedures.GetPageAuthenticated;
import edu.brown.benchmark.wikipedia.procedures.RemoveWatchList;
import edu.brown.benchmark.wikipedia.procedures.UpdatePage;
import edu.brown.benchmark.wikipedia.procedures.UpdateRevisionCounters;

public class WikipediaProjectBuilder extends AbstractProjectBuilder {
    
    // REQUIRED: Retrieved via reflection by BenchmarkController
    public static final Class<? extends BenchmarkComponent> m_clientClass = WikipediaClient.class;
 
    // REQUIRED: Retrieved via reflection by BenchmarkController
    public static final Class<? extends BenchmarkComponent> m_loaderClass = WikipediaLoader.class;
 
    @SuppressWarnings("unchecked")
    public static final Class<? extends VoltProcedure> PROCEDURES[] = (Class<? extends VoltProcedure>[])new Class<?>[] {
        GetTableCounts.class,
        UpdateRevisionCounters.class,
        AddWatchList.class,
        GetPageAnonymous.class,
        GetPageAuthenticated.class,
        RemoveWatchList.class,
        UpdatePage.class,
    };
    
    public static final String PARTITIONING[][] = new String[][] {
        { WikipediaConstants.TABLENAME_LOGGING, "log_page" },
        { WikipediaConstants.TABLENAME_PAGE, "page_id" },
        { WikipediaConstants.TABLENAME_PAGE_RESTRICTIONS, "pr_page" },
        { WikipediaConstants.TABLENAME_RECENTCHANGES, "rc_page" },
        { WikipediaConstants.TABLENAME_REVISION, "rev_page" },
        { WikipediaConstants.TABLENAME_TEXT, "old_page" },
        { WikipediaConstants.TABLENAME_WATCHLIST, "wl_page" },
        { WikipediaConstants.TABLENAME_USER, "user_id" },
        { WikipediaConstants.TABLENAME_USER_GROUPS, "ug_user" }
    };
 
    public WikipediaProjectBuilder() {
        super("wikipedia", WikipediaProjectBuilder.class, PROCEDURES, PARTITIONING);
        
//        addStmtProcedure("testPage", "select * from page");
//        addStmtProcedure("testUser", "select * from USERACCT");
//        addStmtProcedure("testWatchlist", "select * from WATCHLIST");
//        addStmtProcedure("testRevision", "select * from REVISION");
//        addStmtProcedure("testText", "select * from TEXT");
//        addStmtProcedure("testJoin1", "SELECT * " +
//                "  FROM " + WikipediaConstants.TABLENAME_PAGE + ", " +
//                            WikipediaConstants.TABLENAME_REVISION +
//                " WHERE page_id = rev_page " +
//                " AND rev_id = page_latest LIMIT 1"
//                );
//        addStmtProcedure("testJoin2", "SELECT * " +
//                "  FROM " + WikipediaConstants.TABLENAME_PAGE + ", " +
//                            WikipediaConstants.TABLENAME_REVISION +
//                " WHERE rev_id = page_latest LIMIT 1"
//                );
    }

}
