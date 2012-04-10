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
        "old_page,old_text,old_flags" + 
        ") VALUES (" +
        "?,?,?" +
        ")"
    ); 
	public SQLStmt insertRevision = new SQLStmt(
        "INSERT INTO " + WikipediaConstants.TABLENAME_REVISION + " (" +
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
        "?, ?, ?, ?, ?, ?, ?, ?, ?, ?" +
		")"
	);
	public SQLStmt updatePage = new SQLStmt(
        "UPDATE " + WikipediaConstants.TABLENAME_PAGE +
        "   SET page_latest = ?, page_touched = ?, page_is_new = 0, page_is_redirect = 0, page_len = ? " +
        " WHERE page_id = ?"
    );
	public SQLStmt insertRecentChanges = new SQLStmt(
        "INSERT INTO " + WikipediaConstants.TABLENAME_RECENTCHANGES + " (" + 
	    "rc_timestamp, " +
	    "rc_cur_time, " +
	    "rc_namespace, " +
	    "rc_title, " +
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
        "?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?" +
        ")"
    );
	public SQLStmt selectWatchList = new SQLStmt(
        "SELECT wl_user FROM " + WikipediaConstants.TABLENAME_WATCHLIST +
        " WHERE wl_title = ?" +
        "   AND wl_namespace = ?" +
		"   AND wl_user != ?" +
		"   AND wl_notificationtimestamp IS NULL"
    );
	public SQLStmt updateWatchList = new SQLStmt(
        "UPDATE " + WikipediaConstants.TABLENAME_WATCHLIST +
        "   SET wl_notificationtimestamp = ? " +
	    " WHERE wl_title = ?" +
	    "   AND wl_namespace = ?" +
	    "   AND wl_user = ?"
    );
	public SQLStmt selectUser = new SQLStmt(
        "SELECT * FROM " + WikipediaConstants.TABLENAME_USER + 
        " WHERE user_id = ?"
    );
	public SQLStmt insertLogging = new SQLStmt(
        "INSERT INTO " + WikipediaConstants.TABLENAME_LOGGING + " (" +
		"log_type, log_action, log_timestamp, log_user, log_user_text, " +
        "log_namespace, log_title, log_page, log_comment, log_params" +
        ") VALUES (" +
        "?,?,?,?,?,?,?,?,'',?" +
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
	
	public void run( int textId, int pageId,
	                                 String pageTitle, String pageText, int pageNamespace,
	                                 int userId, String userIp, String userText,
	                                 int revisionId, String revComment, int revMinorEdit) {

	    boolean adv;
	    VoltTable rs[] = null;
	    int param = 1; // TODO: REMOVE
	    final TimestampType timestamp = new TimestampType();
	    
	    // INSERT NEW TEXT
		voltQueueSQL(insertText, pageId);
		voltQueueSQL(insertText, param++, pageText);
		voltQueueSQL(insertText, param++, "utf-8");
		rs = voltExecuteSQL();

		adv = rs[0].advanceRow();
		assert(adv) : "Problem inserting new tuples in table text";
		int nextTextId = (int)rs[0].getLong(0); // TODO: Pass in as argument to run()
		
		assert(nextTextId >= 0) : "Invalid nextTextId (" + nextTextId + ")";

		// INSERT NEW REVISION
		param = 1;
		voltQueueSQL(insertRevision, pageId);        // rev_page
		voltQueueSQL(insertRevision, param++, nextTextId);    // rev_text_id
		voltQueueSQL(insertRevision, param++, revComment);        // rev_comment
		voltQueueSQL(insertRevision, param++, revMinorEdit);        // rev_minor_edit
		voltQueueSQL(insertRevision, param++, userId);        // rev_user
		voltQueueSQL(insertRevision, param++, userText);        // rev_user_text
		voltQueueSQL(insertRevision, param++, timestamp);        // rev_timestamp
		voltQueueSQL(insertRevision, param++, 0);        // rev_deleted
		voltQueueSQL(insertRevision, param++, pageText.length());        // rev_len
		voltQueueSQL(insertRevision, param++, revisionId);        // rev_parent_id
		
		rs = voltExecuteSQL();
		
		adv = rs[0].advanceRow();
		int nextRevId = (int)rs[0].getLong(0); // TODO: Should be passed in as argument to run()
		
		assert(nextRevId >= 0) : "Invalid nextRevID (" + nextRevId + ")";

		// I'm removing AND page_latest = "+a.revisionId+" from the query, since
		// it creates sometimes problem with the data, and page_id is a PK
		// anyway
		param = 1;
		voltQueueSQL(updatePage,param++, nextRevId);
		voltQueueSQL(updatePage,param++, timestamp);
		voltQueueSQL(updatePage,param++, pageText.length());
		voltQueueSQL(updatePage,param++, pageId);
		
		//int numUpdatePages = ps.executeUpdate();
		//assert(numUpdatePages == 1) : "WE ARE NOT UPDATING the page table!";

		// REMOVED
		// sql="DELETE FROM `redirect` WHERE rd_from = '"+a.pageId+"';";
		// st.addBatch(sql);
		
		param = 1;
		voltQueueSQL(insertRecentChanges,param++, timestamp);     // rc_timestamp
		voltQueueSQL(insertRecentChanges,param++, timestamp);     // rc_cur_time
		voltQueueSQL(insertRecentChanges,param++, pageNamespace);    // rc_namespace
		voltQueueSQL(insertRecentChanges,param++, pageTitle);     // rc_title
		voltQueueSQL(insertRecentChanges,param++, 0);                // rc_type
		voltQueueSQL(insertRecentChanges,param++, 0);                // rc_minor
		voltQueueSQL(insertRecentChanges,param++, pageId);           // rc_cur_id
		voltQueueSQL(insertRecentChanges,param++, userId);           // rc_user
		voltQueueSQL(insertRecentChanges,param++, userText);      // rc_user_text
		voltQueueSQL(insertRecentChanges,param++, revComment);    // rc_comment
		voltQueueSQL(insertRecentChanges,param++, nextTextId);       // rc_this_oldid
		voltQueueSQL(insertRecentChanges,param++, textId);           // rc_last_oldid
		voltQueueSQL(insertRecentChanges,param++, 0);                // rc_bot
		voltQueueSQL(insertRecentChanges,param++, 0);                // rc_moved_to_ns
		voltQueueSQL(insertRecentChanges,param++, "");            // rc_moved_to_title
		voltQueueSQL(insertRecentChanges,param++, userIp);        // rc_ip
		voltQueueSQL(insertRecentChanges,param++, pageText.length());// rc_old_len
		voltQueueSQL(insertRecentChanges,param++, pageText.length());// rc_new_len
		voltQueueSQL(insertRecentChanges,param++, timestamp);
		voltQueueSQL(insertRecentChanges,param++, timestamp);
		voltQueueSQL(insertRecentChanges,param++, timestamp);
		voltQueueSQL(insertRecentChanges,param++, timestamp);
		
		//int count = ps.executeUpdate();
		//assert(count == 1);

		// REMOVED
		// sql="INSERT INTO `cu_changes` () VALUES ();";
		// st.addBatch(sql);

		// SELECT WATCHING USERS
		param = 1;
		voltQueueSQL(selectWatchList,param++, pageTitle);
		voltQueueSQL(selectWatchList,param++, pageNamespace);
		voltQueueSQL(selectWatchList,param++, userId);
		
		rs = voltExecuteSQL();

		ArrayList<Integer> wlUser = new ArrayList<Integer>();
		while (rs[0].advanceRow()) {
			wlUser.add((int)rs[0].getLong(1));
		}
		

		// =====================================================================
		// UPDATING WATCHLIST: txn3 (not always, only if someone is watching the
		// page, might be part of txn2)
		// =====================================================================
		if (wlUser.isEmpty() == false) {

			// NOTE: this commit is skipped if none is watching the page, and
			// the transaction merge with the following one
		    param = 1;
			voltQueueSQL(updateWatchList,param++, timestamp);
			voltQueueSQL(updateWatchList,param++, pageTitle);
			voltQueueSQL(updateWatchList,param++, pageNamespace);
			
			for (Integer otherUserId : wlUser) {
			    voltQueueSQL(updateWatchList,param, otherUserId.intValue());
				
				//ps.addBatch();// FIXME
			} // FOR
			//ps.executeUpdate();
			voltExecuteSQL();
			// NOTE: this commit is skipped if none is watching the page, and
			// the transaction merge with the following one
			

			// ===================================================================== 
			// UPDATING USER AND LOGGING STUFF: txn4 (might still be part of
			// txn2)
			// =====================================================================

			// This seems to be executed only if the page is watched, and once
			// for each "watcher"
			param = 1;
			
            
			for (Integer otherUserId : wlUser) {
			    voltQueueSQL(selectUser,param, otherUserId.intValue());
				rs = voltExecuteSQL();
				rs[0].advanceRow();
				
			} // FOR
		}

		// This is always executed, sometimes as a separate transaction,
		// sometimes together with the previous one
		param = 1;
		voltQueueSQL(insertLogging, WikipediaConstants.UPDATEPAGE_LOG_TYPE,
		                            WikipediaConstants.UPDATEPAGE_LOG_ACTION,
		                            timestamp,
		                            userId,
		                            pageTitle,
		                            pageNamespace,
		                            userText,
		                            pageId,
		                            String.format("%d\n%d\n%d", nextRevId, revisionId, 1));
		//ps.executeUpdate();
		voltExecuteSQL();
		
		
		param = 1;
		voltQueueSQL(updateUserEdit,param++, userId);
		//ps.executeUpdate();
		voltExecuteSQL();
		
		param = 1;
		voltQueueSQL(updateUserTouched,param++, timestamp);
		voltQueueSQL(updateUserTouched,param++, userId);
		
		//ps.executeUpdate();
		voltExecuteSQL();
	}
}
