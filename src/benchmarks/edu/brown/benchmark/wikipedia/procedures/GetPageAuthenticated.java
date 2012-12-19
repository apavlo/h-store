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
import org.voltdb.VoltProcedure;
import org.voltdb.SQLStmt;
import org.voltdb.VoltTable;

import edu.brown.benchmark.wikipedia.WikipediaConstants;

@ProcInfo(
    partitionInfo = "PAGE.PAGE_ID: 0",
    singlePartition = false
)
public class GetPageAuthenticated extends VoltProcedure {
	
    // -----------------------------------------------------------------
    // STATEMENTS
    // -----------------------------------------------------------------
    
    public SQLStmt selectPageRestriction = new SQLStmt(
        "SELECT * FROM " + WikipediaConstants.TABLENAME_PAGE_RESTRICTIONS + 
        " WHERE pr_page = ?"
    );
    public SQLStmt selectIpBlocks = new SQLStmt(
        "SELECT * FROM " + WikipediaConstants.TABLENAME_IPBLOCKS +
        " WHERE ipb_user = ?"
    );
    public SQLStmt selectPageRevision = new SQLStmt(
        "SELECT * " +
        "  FROM " + WikipediaConstants.TABLENAME_REVISION +
        " WHERE rev_page = ? " +
        "   AND rev_id = ? "
    );
    public SQLStmt selectPage = new SQLStmt(
        "SELECT page_title, page_latest, page_restrictions " +
        "  FROM " + WikipediaConstants.TABLENAME_PAGE  +
        " WHERE page_id = ? " +
        "   AND page_namespace = ?"
    );
    public SQLStmt selectText = new SQLStmt(
        "SELECT old_text, old_flags FROM " + WikipediaConstants.TABLENAME_TEXT +
        " WHERE old_page = ? " +
        "   AND old_id = ? "
    );
	public SQLStmt selectUser = new SQLStmt(
        "SELECT * FROM " + WikipediaConstants.TABLENAME_USER + 
        " WHERE user_id = ? LIMIT 1"
    );
	public SQLStmt selectGroup = new SQLStmt(
        "SELECT ug_group FROM " + WikipediaConstants.TABLENAME_USER_GROUPS + 
        " WHERE ug_user = ?"
    );

    // -----------------------------------------------------------------
    // RUN
    // -----------------------------------------------------------------
	
    public VoltTable run(long pageId, int pageNamespace, int userId, String userIp, boolean forSelect) {
        assert (userId >= 0);
        VoltTable rs[] = null;
        
        // Retrieve the user data, if the user is logged in
        voltQueueSQL(selectPageRestriction, pageId);
        voltQueueSQL(selectIpBlocks, userId);
        voltQueueSQL(selectUser, userId);
        voltQueueSQL(selectGroup, userId);
        voltQueueSQL(selectPage, pageId, pageNamespace);
        
        rs = voltExecuteSQL();
        
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
        
        // Double check that we have a valid userId
        if (rs[2].advanceRow() == false) {
            throw new VoltAbortException("Invalid UserId: " + userId);
        }
        String user_text = rs[2].getString(2);

        // Fetch all groups the user might belong to (access control information)
        while (rs[3].advanceRow()) {
            @SuppressWarnings("unused")
            String userGroupName = rs[3].getString(0);
        }
        
        // Check that we have a page
        if (rs[4].advanceRow() == false) {
            String msg = String.format("Invalid pageId %d", pageId);
            throw new VoltAbortException(msg);
        }
        int col = 0;
        String pageTitle = rs[4].getString(col++);
        long pageLatest = rs[4].getLong(col++);

        // Grab Page Revision + Text
        voltQueueSQL(selectPageRevision, pageId, pageId);
        voltQueueSQL(selectText, pageId, pageLatest);
        rs = voltExecuteSQL();
        if (rs[0].advanceRow() == false) {
            String msg = String.format("No such revision revId=%d exists for pageId:%d / pageNamespace:%d",
                                       pageLatest, pageId, pageNamespace);
            throw new VoltAbortException(msg);
        }
        long revisionId = rs[0].getLong(0);
        long textId = rs[0].getLong(2);
        assert(rs[0].advanceRow() == false);

        // NOTE: the following is our variation of wikipedia... the original did
        // not contain old_page column!
        if (rs[1].advanceRow() == false) {
            String msg = "No such text: " + textId + " for page_id:" + pageId + " page_namespace: " + pageNamespace;
            throw new VoltAbortException(msg);
        }
        String old_text = rs[1].getString(0);
        
        VoltTable result = new VoltTable(WikipediaConstants.GETPAGE_OUTPUT_COLS);
        if (forSelect == false) {
            result.addRow(pageId,       // PAGE_ID
                          pageTitle,    // PAGE_TITLE
                          old_text,     // OLD_TEXT
                          textId,       // TEXT_ID
                          revisionId,   // REVISION_ID
                          user_text);   // USER_TEXT
        }
        assert !rs[0].advanceRow();
        return (result);
    }

}
