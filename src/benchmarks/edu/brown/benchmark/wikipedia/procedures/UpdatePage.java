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

import java.util.ArrayList;

import org.voltdb.VoltProcedure;
import org.voltdb.SQLStmt;
import org.voltdb.VoltTable;
import org.voltdb.types.TimestampType;

import edu.brown.benchmark.wikipedia.WikipediaConstants;

public class UpdatePage extends VoltProcedure {
	
    // -----------------------------------------------------------------
    // STATEMENTS
    // -----------------------------------------------------------------
    
	public SQLStmt insertText = new SQLStmt(
        "INSERT INTO " + WikipediaConstants.TABLENAME_TEXT + " (" +
        "old_id,old_page,old_text,old_flags" + 
        ") VALUES (" +
        "?,?,?,?" +
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
        "   SET page_latest = ?, page_touched = ?, page_is_new = 0, page_is_redirect = 0, page_len = ? " +
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
		"log_id,log_type, log_action, log_timestamp, log_user, log_user_text, " +
        "log_namespace, log_title, log_page, log_comment, log_params" +
        ") VALUES (" +
        "?,?,?,?,?,?,?,?,?,'',?" +
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
	
	public long run(int nextId, int textId, int pageId, int pageNamespace, String pageText,
	                int userId, String userIp, String userText,
	                int revisionId, String revComment, int revMinorEdit) {

	    boolean adv;
	    VoltTable rs[] = null;
	    final TimestampType timestamp = new TimestampType();
	    
	    // INSERT NEW TEXT
		voltQueueSQL(insertText, nextId, pageId, pageText, "utf-8");
		rs = voltExecuteSQL();
		adv = rs[0].advanceRow();
		assert(adv) : "Problem inserting new tuples in table text";
		
        int nextTextId = (int) rs[0].getLong(0);
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
		int nextRevId = (int)rs[0].getLong(0); // TODO: Should be passed in as argument to run()
		
		assert(nextRevId >= 0) : "Invalid nextRevID (" + nextRevId + ")";

		// I'm removing AND page_latest = "+a.revisionId+" from the query, since
		// it creates sometimes problem with the data, and page_id is a PK
		// anyway
		voltQueueSQL(updatePage,nextRevId,
		                        timestamp,
		                        pageText.length(), 
		                        pageId);
		
		// REMOVED
		// sql="DELETE FROM `redirect` WHERE rd_from = '"+a.pageId+"';";
		// st.addBatch(sql);
		
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
		                                  pageText.length()
		                                  );
		
		// SELECT WATCHING USERS
		voltQueueSQL(selectWatchList, pageId, pageNamespace, userId, null);
		
		rs = voltExecuteSQL();

		ArrayList<Integer> wlUser = new ArrayList<Integer>();
		while (rs[0].advanceRow()) {
			wlUser.add((int)rs[0].getLong(0));
		}
		

		// =====================================================================
		// UPDATING WATCHLIST: txn3 (not always, only if someone is watching the
		// page, might be part of txn2)
		// =====================================================================
		if (wlUser.isEmpty() == false) {

			// NOTE: this commit is skipped if none is watching the page, and
			// the transaction merge with the following one
			
			for (Integer otherUserId : wlUser) {
			    voltQueueSQL(updateWatchList, timestamp, pageId, pageNamespace, otherUserId.intValue());
			    voltExecuteSQL();
			} // FOR
			
			// NOTE: this commit is skipped if none is watching the page, and
			// the transaction merge with the following one
			

			// ===================================================================== 
			// UPDATING USER AND LOGGING STUFF: txn4 (might still be part of
			// txn2)
			// =====================================================================

			// This seems to be executed only if the page is watched, and once
			// for each "watcher"
			
            
			for (Integer otherUserId : wlUser) {
			    voltQueueSQL(selectUser, otherUserId.intValue());
				rs = voltExecuteSQL();
				rs[0].advanceRow();
				
			} // FOR
		}

		// This is always executed, sometimes as a separate transaction,
		// sometimes together with the previous one
		voltQueueSQL(insertLogging, nextId,
		                            WikipediaConstants.UPDATEPAGE_LOG_TYPE,
		                            WikipediaConstants.UPDATEPAGE_LOG_ACTION,
		                            timestamp,
		                            userId,
		                            pageId,
		                            pageNamespace,
		                            userText,
		                            pageId,
		                            String.format("%d\n%d\n%d", nextRevId, revisionId, 1));
		voltQueueSQL(updateUserEdit, userId);
		voltQueueSQL(updateUserTouched, timestamp, userId);
		
		voltExecuteSQL();
		
		return (1);
	}
}
