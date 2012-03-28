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

import org.voltdb.VoltProcedure;
import org.voltdb.SQLStmt;
import org.voltdb.VoltTable;

import edu.brown.benchmark.wikipedia.WikipediaConstants;
import edu.brown.benchmark.wikipedia.util.Article;

public class GetPageAuthenticated extends VoltProcedure {
	
    // -----------------------------------------------------------------
    // STATEMENTS
    // -----------------------------------------------------------------
    
    public SQLStmt selectPage = new SQLStmt(
        "SELECT * FROM " + WikipediaConstants.TABLENAME_PAGE + 
        " WHERE page_namespace = ? AND page_title = ? LIMIT 1"
    );
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
        "  FROM " + WikipediaConstants.TABLENAME_PAGE + ", " +
                    WikipediaConstants.TABLENAME_REVISION +
        " WHERE page_id = rev_page " +
        "   AND rev_page = ? " +
        "   AND page_id = ? " +
        "   AND rev_id = page_latest LIMIT 1"
    );
    public SQLStmt selectText = new SQLStmt(
        "SELECT old_text,old_flags FROM " + WikipediaConstants.TABLENAME_TEXT +
        " WHERE old_id = ? LIMIT 1"
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
	
    public Article run( boolean forSelect, String userIp, int userId, int nameSpace, String pageTitle) {
        // =======================================================
        // LOADING BASIC DATA: txn1
        // =======================================================
        // Retrieve the user data, if the user is logged in

        // FIXME TOO FREQUENTLY SELECTING BY USER_ID
        String userText = userIp;
        
        if (userId > 0) {
            voltQueueSQL(selectUser,1, userId);
            VoltTable rs[] = voltExecuteSQL();
            if (rs[0].advanceRow()) {
                userText = rs[0].getString("user_name");
            } else {
                throw new VoltAbortException("Invalid UserId: " + userId);
            }
            
            // Fetch all groups the user might belong to (access control
            // information)
            voltQueueSQL(selectGroup,1, userId);
            
            rs = voltExecuteSQL();
            while (rs[0].advanceRow()) {
                @SuppressWarnings("unused")
                String userGroupName = rs[0].getString(0);
            }
            
        }

        voltQueueSQL(selectPage,1, nameSpace);
        voltQueueSQL(selectPage,2, pageTitle);
        VoltTable rs[] = voltExecuteSQL();

        if (!rs[0].advanceRow()) {
            
            throw new VoltAbortException("INVALID page namespace/title:" + nameSpace + "/" + pageTitle);
        }
        int pageId = (int)rs[0].getLong("page_id");
        assert !rs[0].advanceRow();
        

        voltQueueSQL(selectPageRestriction,1, pageId);
        rs = voltExecuteSQL();
        while (rs[0].advanceRow()) {
            String pr_type = rs[0].getString(0);
            assert(pr_type != null);
        }
        
        
        // check using blocking of a user by either the IP address or the
        // user_name
        voltQueueSQL(selectIpBlocks,1, userId);
        rs = voltExecuteSQL();
        while (rs[0].advanceRow()) {
            String ipb_expiry = rs[0].getString(10);
            assert(ipb_expiry != null);
        }
        

        voltQueueSQL(selectPageRevision,1, pageId);
        voltQueueSQL(selectPageRevision,2, pageId);
        rs = voltExecuteSQL();
        if (!rs[0].advanceRow()) {
            throw new VoltAbortException("no such revision: page_id:" + pageId + " page_namespace: " + nameSpace + " page_title:" + pageTitle);
        }

        int revisionId = (int)rs[0].getLong("rev_id");
        int textId = (int)rs[0].getLong("rev_text_id");
        assert !rs[0].advanceRow();
        

        // NOTE: the following is our variation of wikipedia... the original did
        // not contain old_page column!
        // sql =
        // "SELECT old_text,old_flags FROM `text` WHERE old_id = '"+textId+"' AND old_page = '"+pageId+"' LIMIT 1";
        // For now we run the original one, which works on the data we have
        voltQueueSQL(selectText,1, textId);
        rs = voltExecuteSQL();
        if (!rs[0].advanceRow()) {
            throw new VoltAbortException("no such text: " + textId + " for page_id:" + pageId + " page_namespace: " + nameSpace + " page_title:" + pageTitle);
        }
        Article a = null;
        if (!forSelect)
            a = new Article(userText, pageId, rs[0].getString("old_text"), textId, revisionId);
        assert !rs[0].advanceRow();
        
        // FIXME can not return Article, return voltTable

        return a;
    }

}
