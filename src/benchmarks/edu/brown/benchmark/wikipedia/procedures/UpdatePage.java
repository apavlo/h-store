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
import org.voltdb.types.TimestampType;

import edu.brown.benchmark.wikipedia.WikipediaConstants;

@ProcInfo(
    partitionInfo = "PAGE.PAGE_ID: 0",
    singlePartition = false
)
public class UpdatePage extends VoltProcedure {
	
    // -----------------------------------------------------------------
    // STATEMENTS
    // -----------------------------------------------------------------
    
    public SQLStmt selectLastPageRevision = new SQLStmt(
        "SELECT page_latest FROM " + WikipediaConstants.TABLENAME_PAGE +
        " WHERE page_id = ?"
    );
    
	public SQLStmt insertText = new SQLStmt(
        "INSERT INTO " + WikipediaConstants.TABLENAME_TEXT + " (" +
        "old_id, " +
        "old_page, " +
        "old_text, " +
        "old_flags" + 
        ") VALUES (" +
        "?, " + // old_id
        "?, " + // old_page
        "?, " + // old_text
        "?" +   // old_flags
        ")"
    ); 
	public SQLStmt insertRevision = new SQLStmt(
        "INSERT INTO " + WikipediaConstants.TABLENAME_REVISION + " (" +
        "rev_id," +
		"rev_page, " +
		"rev_text_id, " +
		"rev_comment, " +
		"rev_minor_edit, " +
		"rev_user, " +
        "rev_user_text, " +
        "rev_timestamp, " +
        "rev_deleted, " +
        "rev_len, " +
        "rev_parent_id" +
		") VALUES (" +
        "?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?" +
		")"
	);
	public SQLStmt updatePage = new SQLStmt(
        "UPDATE " + WikipediaConstants.TABLENAME_PAGE +
        "   SET page_latest = page_latest + 1, " +
        "       page_touched = ?, " +
        "       page_is_new = 0, " +
        "       page_is_redirect = 0, " +
        "       page_len = ? " +
        " WHERE page_id = ?"
    );
	public SQLStmt insertRecentChanges = new SQLStmt(
        "INSERT INTO " + WikipediaConstants.TABLENAME_RECENTCHANGES + " (" +
        "rc_id, " +
	    "rc_timestamp, " +
	    "rc_cur_time, " +
	    "rc_namespace, " +
	    "rc_page, " +
	    "rc_type, " +
        "rc_minor, " +
        "rc_cur_id, " +
        "rc_user, " +
        "rc_user_text, " +
        "rc_comment, " +
        "rc_this_oldid, " +
	    "rc_last_oldid, " +
	    "rc_bot, " +
	    "rc_moved_to_ns, " +
	    "rc_moved_to_title, " +
	    "rc_ip, " +
        "rc_old_len, " +
        "rc_new_len " +
        ") VALUES (" +
        "?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?" +
        ")"
    );
	public SQLStmt selectWatchList = new SQLStmt(
        "SELECT wl_user FROM " + WikipediaConstants.TABLENAME_WATCHLIST +
        " WHERE wl_page = ?" +
        "   AND wl_namespace = ?" +
		"   AND wl_user != ?" +
		"   AND wl_notificationtimestamp = ?"
    );
	public SQLStmt updateWatchList = new SQLStmt(
        "UPDATE " + WikipediaConstants.TABLENAME_WATCHLIST +
        "   SET wl_notificationtimestamp = ? " +
	    " WHERE wl_page = ?" +
	    "   AND wl_namespace = ?" +
	    "   AND wl_user = ?"
    );
	public SQLStmt selectUser = new SQLStmt(
        "SELECT * FROM " + WikipediaConstants.TABLENAME_USER + 
        " WHERE user_id = ?"
    );
	public SQLStmt insertLogging = new SQLStmt(
        "INSERT INTO " + WikipediaConstants.TABLENAME_LOGGING + " (" +
		"log_id," +
		"log_type, " +
		"log_action, " +
		"log_timestamp, " +
		"log_user, " +
		"log_namespace, " +
		"log_page, " +
		"log_comment, " +
		"log_params, " +
		"log_user_text " +
        ") VALUES (" +
        "?," +  // log_id
        "?," +  // log_type
        "?," +  // log_action
        "?," +  // log_timestamp
        "?," +  // log_user
        "?," +  // log_namespace
        "?," +  // log_page
        "''," +  // log_comment
        "?," +  // log_params
        "?" +   // log_user_text
        ")"
    );
	public SQLStmt updateUserEdit = new SQLStmt(
        "UPDATE " + WikipediaConstants.TABLENAME_USER +
        "   SET user_editcount=user_editcount+1" +
        " WHERE user_id = ?"
    );
	public SQLStmt updateUserTouched = new SQLStmt(
        "UPDATE " + WikipediaConstants.TABLENAME_USER + 
        "   SET user_touched = ?" +
        " WHERE user_id = ?"
    );
	
    // -----------------------------------------------------------------
    // RUN
    // -----------------------------------------------------------------
	
	public long run(long pageId, int textId, int pageNamespace, String pageText,
	                int userId, String userIp, String userText,
	                int revisionId, String revComment, int revMinorEdit) {

	    boolean adv;
	    VoltTable rs[] = null;
	    final TimestampType timestamp = new TimestampType();
	    
	    // Get the next id from the PAGE
	    voltQueueSQL(selectLastPageRevision, pageId);
	    rs = voltExecuteSQL();
	    int nextId = (int)rs[0].asScalarLong() + 1;
	    
	    // INSERT NEW TEXT
		voltQueueSQL(insertText, nextId, pageId, pageText, "utf-8");
		rs = voltExecuteSQL();
		adv = rs[0].advanceRow();
		assert(adv) : "Problem inserting new tuples in table text";
        int nextTextId = (int)rs[0].getLong(0);
        
		// INSERT NEW REVISION
		voltQueueSQL(insertRevision, nextId,
		                             pageId, 
		                             nextTextId,
		                             revComment, 
		                             revMinorEdit,
		                             userId, 
		                             userText,
		                             timestamp,
		                             0,
		                             pageText.length(),
		                             revisionId);		
		rs = voltExecuteSQL();
		adv = rs[0].advanceRow();

		// I'm removing AND page_latest = "+a.revisionId+" from the query, since
		// it creates sometimes problem with the data, and page_id is a PK
		// anyway
		voltQueueSQL(updatePage, timestamp, pageText.length(), pageId);
		voltQueueSQL(insertRecentChanges, nextId,
		                                  timestamp, 
		                                  new TimestampType(),
		                                  pageNamespace,
		                                  pageId,
		                                  0, 
		                                  0,
		                                  pageId, 
		                                  userId, 
		                                  userText,
		                                  revComment,
		                                  nextTextId,
		                                  textId,
		                                  0, 
		                                  0, 
		                                  "", 
		                                  userIp,
		                                  pageText.length(),
		                                  pageText.length());
		
		// SELECT WATCHING USERS
		voltQueueSQL(selectWatchList, pageId, pageNamespace, userId, null);
		rs = voltExecuteSQL();

		// =====================================================================
		// UPDATING WATCHLIST: txn3 (not always, only if someone is watching the
		// page, might be part of txn2)
		// =====================================================================
		if (rs[0].getRowCount() > 0) {

			// NOTE: this commit is skipped if none is watching the page, and
			// the transaction merge with the following one
		
		    while (rs[0].advanceRow()) {
	            int otherUserId = (int)rs[0].getLong(0);
			    voltQueueSQL(updateWatchList, timestamp, pageId, pageNamespace, otherUserId);
			    voltExecuteSQL();
			} // WHILE
			
			// ===================================================================== 
			// UPDATING USER AND LOGGING STUFF: txn4 (might still be part of
			// txn2)
			// =====================================================================

			// This seems to be executed only if the page is watched, and once
			// for each "watcher"
			
		    rs[0].resetRowPosition();
		    while (rs[0].advanceRow()) {
                int otherUserId = (int)rs[0].getLong(0);
			    voltQueueSQL(selectUser, otherUserId);
			} // WHILE
		    rs = voltExecuteSQL();
		    while (rs[0].advanceRow()) {
		        // Nothing to do
		    } // WHILE
		    
		}

		// This is always executed, sometimes as a separate transaction,
		// sometimes together with the previous one
		long logId = pageId | ((long)nextId)<<32;
		String logParams = String.format("%d -- %d -- %d", nextId, revisionId, 1);
		voltQueueSQL(insertLogging, logId,                                    // log_id
		                            WikipediaConstants.UPDATEPAGE_LOG_TYPE,   // log_type
		                            WikipediaConstants.UPDATEPAGE_LOG_ACTION, // log_action
		                            timestamp,                                // log_timestamp
		                            userId,                                   // log_user
		                            pageNamespace,                            // log_namespace
		                            pageId,                                   // log_page
		                            logParams,                                // log_params
		                            userText);                                // log_user_text
		voltQueueSQL(updateUserEdit, userId);
		voltQueueSQL(updateUserTouched, timestamp, userId);
		voltExecuteSQL(true);
		
		return (1);
	}
}
