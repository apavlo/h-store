/*******************************************************************************
 * oltpbenchmark.com
 *  
 *  Project Info:  http://oltpbenchmark.com
 *  Project Members:    Carlo Curino <carlo.curino@gmail.com>
 *              Evan Jones <ej@evanjones.ca>
 *              DIFALLAH Djellel Eddine <djelleleddine.difallah@unifr.ch>
 *              Andy Pavlo <pavlo@cs.brown.edu>
 *              CUDRE-MAUROUX Philippe <philippe.cudre-mauroux@unifr.ch>  
 *                  Yang Zhang <yaaang@gmail.com> 
 * 
 *  This library is free software; you can redistribute it and/or modify it under the terms
 *  of the GNU General Public License as published by the Free Software Foundation;
 *  either version 3.0 of the License, or (at your option) any later version.
 * 
 *  This library is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 *  without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 *  See the GNU Lesser General Public License for more details.
 ******************************************************************************/
package edu.brown.benchmark.wikipedia.procedures;

import org.voltdb.ProcInfo;
import org.voltdb.SQLStmt;
import org.voltdb.VoltProcedure;
import org.voltdb.VoltTable;
import org.voltdb.types.TimestampType;

import java.util.ArrayList; 
import java.util.Random; 
import edu.brown.rand.RandomDistribution.Zipf; 

import edu.brown.benchmark.wikipedia.WikipediaConstants;

@ProcInfo(
    partitionInfo = "PAGE.PAGE_TITLE: 3"
)
public class GetPageAnonymous extends VoltProcedure {
	
    // -----------------------------------------------------------------
    // STATEMENTS
    // -----------------------------------------------------------------
    
	public SQLStmt selectPageRestriction = new SQLStmt(
        "SELECT * FROM " + WikipediaConstants.TABLENAME_PAGE_RESTRICTIONS +
        " WHERE pr_page = ?"
    );
	// XXX this is hard for translation
	public SQLStmt selectIpBlocks = new SQLStmt(
        "SELECT * FROM " + WikipediaConstants.TABLENAME_IPBLOCKS + 
        " WHERE ipb_address = ?"
    ); 
    
    public SQLStmt selectRevisionsByPage = new SQLStmt(
        "SELECT rev_id FROM " + WikipediaConstants.TABLENAME_REVISION + " WHERE page_id = ?");
    
	public SQLStmt selectPageRevision = new SQLStmt(
        "SELECT * " +
	    "  FROM " + WikipediaConstants.TABLENAME_PAGE + ", " +
	                WikipediaConstants.TABLENAME_REVISION +
	    " WHERE page_id = rev_page " +
        "   AND rev_page = ? " +
	    "   AND page_id = ? " +
        "   AND rev_id = page_latest LIMIT 1"
    );
	public SQLStmt selectPage = new SQLStmt(
	        "SELECT page_latest " +
	        " FROM " + WikipediaConstants.TABLENAME_PAGE  +
	        " WHERE page_id = ?"
	);
	public SQLStmt selectRevision = new SQLStmt(
            "SELECT * " +
            " FROM " + WikipediaConstants.TABLENAME_REVISION +
            " WHERE " +
	        "rev_id = ? LIMIT 1"
    );
    
    public SQLStmt selectPageRevisionNotLatest = new SQLStmt(
        "SELECT * " +
        "  FROM " + WikipediaConstants.TABLENAME_PAGE + ", " +
        WikipediaConstants.TABLENAME_REVISION +
        " WHERE page_id = rev_page " +
        "   AND rev_page = ? " +
        "   AND page_id = ? " +
        "   AND rev_id = ? LIMIT 1"
    );
	
	public SQLStmt selectText = new SQLStmt(
        "SELECT old_text, old_flags FROM " + WikipediaConstants.TABLENAME_TEXT +
        " WHERE old_id = ? LIMIT 1"
    );
    
    private final Random rnd = new Random(); 
    
    private boolean flipCoin(double phead)
    {
        assert phead <= 1.0; 
        return rnd.nextDouble() <= phead; 
    }

	// -----------------------------------------------------------------
    // RUN
    // -----------------------------------------------------------------
	
	public VoltTable run(int pageId, boolean forSelect, String userIp, int pageNamespace, String pageTitle) {		

        voltQueueSQL(selectPageRestriction, pageId);
        voltQueueSQL(selectIpBlocks, userIp);
        //voltQueueSQL(selectPageRevision, pageId, pageId);
        VoltTable rs[] = voltExecuteSQL();
        //assert(rs.length == 3):"length expected is:3, but is:" + rs.length;
        assert(rs.length == 2):"length expected is:2, but is:" + rs.length;
        
        // Grab Page Restrictions
        while (rs[0].advanceRow()) {
            String pr_type = rs[0].getString(0);
            assert(pr_type != null);
        } // WHILE
        
        // check using blocking of a user by either the IP address or the
        // user_name
        while (rs[1].advanceRow()) {
            String ipb_expiry = rs[1].getString(10);
            assert(ipb_expiry != null);
        } // WHILE
        
        int revisionId, textId;
        if (flipCoin(WikipediaConstants.PAST_REV_CHECK_PROB)) {
            
            // get a list of all past revisions
            voltQueueSQL(selectRevisionsByPage, pageId); 
            rs = voltExecuteSQL(); 
            
            if (!rs[0].advanceRow()) {
                String msg = String.format("Invalid Page: Missing revision Namespace:%d / Title:--%s-- / PageId:%d",
                                           pageNamespace, pageTitle, pageId);
                throw new VoltAbortException(msg);
            }
            
            ArrayList<Integer> revIds = new ArrayList<Integer>();
            while(rs[0].advanceRow())
            {
                revIds.add((int)rs[0].getLong(1));
            }
            
            int revId;
            if (revIds.size() == 1) {
                // only one rev, so we must pick it
                revId = revIds.get(0);
            } else {
                
                // revIds.size() - 1 so we exclude the latest revision (we want to make
                // this branch load *not* the latest page
                Zipf zip = new Zipf(rnd, 0, revIds.size() - 1, WikipediaConstants.PAST_REV_ZIPF_SKEW); 
                
                // pick the index into revIds
                int idx = (int)zip.nextLong();
                assert idx >= 0 && idx < (revIds.size() - 1);
                
                // index from the end, since we want to favor latest revisions
                revId = revIds.get( (revIds.size() - 2) - idx );
            }
            
            voltQueueSQL(selectPageRevisionNotLatest, pageId, pageId, revId); 
            rs = voltExecuteSQL(); 
            
            if (!rs[0].advanceRow()) {
                String msg = String.format("Invalid Page: Missing revision Namespace:%d / Title:--%s-- / PageId:%d",
                                           pageNamespace, pageTitle, pageId);
                throw new VoltAbortException(msg);
            }
            
            revisionId = (int)rs[0].getLong("rev_id");
            textId = (int)rs[0].getLong("rev_text_id");
            
            assert !rs[0].advanceRow();
        } 
        else { // we are only interested in the latest page
            
            //        System.err.println("selectPage:\n" + rs[0]);
            //        System.err.println("selectRevision:\n" + rs[1]);
            //        System.err.println("selectPageRevision, PageId:" + pageId);
            //        voltQueueSQL(selectPageRevision, pageId, pageId);
            voltQueueSQL(selectPageRevision,pageId, pageId);
            rs = voltExecuteSQL();
            
            if (!rs[0].advanceRow()) {
                String msg = String.format("Invalid Page: Missing revision Namespace:%d / Title:--%s-- / PageId:%d",
                                           pageNamespace, pageTitle, pageId);
                throw new VoltAbortException(msg);
            }
            
            revisionId = (int)rs[0].getLong("rev_id");
            textId = (int)rs[0].getLong("rev_text_id");
            
            assert !rs[0].advanceRow();
        }

        // NOTE: the following is our variation of wikipedia... the original did
        // not contain old_page column!
        // "SELECT old_text,old_flags FROM `text` WHERE old_id = '"+textId+"' AND old_page = '"+pageId+"' LIMIT 1";
        // For now we run the original one, which works on the data we have
        voltQueueSQL(selectText, textId);
        rs = voltExecuteSQL();
        if (!rs[0].advanceRow()) {
            String msg = "No such text: " + textId + " for page_id:" + pageId + " page_namespace: " + pageNamespace + " page_title:" + pageTitle;
            throw new VoltAbortException(msg);
        }
        
        VoltTable result = new VoltTable(WikipediaConstants.GETPAGE_OUTPUT_COLS);
        if (forSelect == false) {
            result.addRow(userIp,
                          pageId,
                          rs[0].getString("old_text"),
                          textId,
                          revisionId);
        }
        assert !rs[0].advanceRow();
        return (result);
    }

}
