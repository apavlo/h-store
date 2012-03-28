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

import org.voltdb.SQLStmt;
import org.voltdb.VoltProcedure;
import org.voltdb.types.TimestampType;

import edu.brown.benchmark.wikipedia.WikipediaConstants;

public class AddWatchList extends VoltProcedure {

    // -----------------------------------------------------------------
    // STATEMENTS
    // -----------------------------------------------------------------
    
    public SQLStmt insertWatchList = new SQLStmt(
        "INSERT INTO " + WikipediaConstants.TABLENAME_WATCHLIST + " (" + 
        "wl_user, wl_namespace, wl_title, wl_notificationtimestamp" +
        ") VALUES (" +
        "?,?,?,NULL" +
        ")"
    );
   
    public SQLStmt setUserTouched = new SQLStmt(
        "UPDATE " + WikipediaConstants.TABLENAME_USER + 
        "   SET user_touched = ? WHERE user_id = ?"
    );
    
    // -----------------------------------------------------------------
    // RUN
    // -----------------------------------------------------------------
	
    public void run( int userId, int nameSpace, String pageTitle) throws VoltAbortException {
		if (userId > 0) {
		    // TODO: find a way to by pass Unique constraints in SQL server (Replace, Merge ..?)
		    // Here I am simply catching the right excpetion and move on.
    			voltQueueSQL(insertWatchList,1, userId);
    			voltQueueSQL(insertWatchList,2, nameSpace);
    			voltQueueSQL(insertWatchList,3, pageTitle);
    			voltExecuteSQL();
		
			if (nameSpace == 0) 
			{ 
    				// if regular page, also add a line of
    				// watchlist for the corresponding talk page
    			    voltQueueSQL(insertWatchList,1, userId);
                    voltQueueSQL(insertWatchList,2, 1);
                    voltQueueSQL(insertWatchList,3, pageTitle);
                    voltExecuteSQL();
			}
			voltQueueSQL(setUserTouched,1, new TimestampType());
			voltQueueSQL(setUserTouched,2, userId);
			
			voltExecuteSQL();
		}
	}
    
}
