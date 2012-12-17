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
import org.voltdb.exceptions.ConstraintFailureException;
import org.voltdb.types.TimestampType;

import edu.brown.benchmark.wikipedia.WikipediaConstants;

@ProcInfo(
    partitionInfo = "USERACCT.USER_ID: 0",
    singlePartition = true
)
public class AddWatchList extends VoltProcedure {
 
    // -----------------------------------------------------------------
    // STATEMENTS
    // -----------------------------------------------------------------

    public SQLStmt insertWatchList = new SQLStmt(
        "INSERT INTO " + WikipediaConstants.TABLENAME_WATCHLIST + " (" + 
        "wl_user, " +
        "wl_namespace, " +
        "wl_page, " +
        "wl_notificationtimestamp" +
        ") VALUES (" +
        "?," +  // wl_user
        "?," +  // wl_namespace
        "?," +  // wl_page
        "?" +   // wl_notificationtimestamp
        ")"
    );
   
    public SQLStmt setUserTouched = new SQLStmt(
        "UPDATE " + WikipediaConstants.TABLENAME_USER + 
        "   SET user_touched = ? WHERE user_id = ?"
    );

    // -----------------------------------------------------------------
    // RUN
    // -----------------------------------------------------------------

    public VoltTable[] run(int userId, int nameSpace, long pageId) throws VoltAbortException {
        final TimestampType timestamp = new TimestampType();
        
        if (userId > 0) {
            // TODO: find a way to by pass Unique constraints in SQL server
            // (Replace, Merge ..?)
            // Here I am simply catching the right excpetion and move on.
            
            voltQueueSQL(insertWatchList, userId, nameSpace, pageId, null);

            if (nameSpace == 0) {
                // if regular page, also add a line of
                // watchlist for the corresponding talk page
                voltQueueSQL(insertWatchList, userId, 1,  pageId,  null);
            }
            voltQueueSQL(setUserTouched, timestamp, userId);
        }
        VoltTable result[] = null;
        try {
            result = voltExecuteSQL(true);
        } catch (ConstraintFailureException ex) {
            String msg = String.format("Duplicate watchlist record on pageId:%d for userId:%d",
                                       pageId, userId);
            throw new VoltAbortException(msg);
        }
        return (result);
    }

}
