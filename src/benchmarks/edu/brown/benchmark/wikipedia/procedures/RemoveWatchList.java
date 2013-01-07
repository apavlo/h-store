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
    partitionInfo = "USERACCT.USER_ID: 0",
    singlePartition = true
)
public class RemoveWatchList extends VoltProcedure {
	
	public SQLStmt removeWatchList = new SQLStmt(
        "DELETE FROM " + WikipediaConstants.TABLENAME_WATCHLIST +
        " WHERE wl_user = ? " +
        "   AND wl_namespace = ? " +
        "   AND wl_page = ?"
    );
    public SQLStmt setUserTouched = new SQLStmt(
        "UPDATE " + WikipediaConstants.TABLENAME_USER +
        "   SET user_touched = ? " +
        " WHERE user_id =  ? "
    ); 

    public VoltTable[] run(int userId, int nameSpace, long pageId) {
        final TimestampType timestamp = new TimestampType();
        if (userId > 0) {
            voltQueueSQL(removeWatchList, userId, nameSpace, pageId);
            
            if (nameSpace == 0) {
                // if regular page, also remove a line of
                // watchlist for the corresponding talk page
                voltQueueSQL(removeWatchList, userId, 1, pageId);
            }
                        
            voltQueueSQL(setUserTouched, timestamp, userId);
        }
        return (voltExecuteSQL(true));
    }
}
