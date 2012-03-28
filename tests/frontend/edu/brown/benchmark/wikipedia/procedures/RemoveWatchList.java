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

import com.sun.jmx.snmp.Timestamp;

import edu.brown.benchmark.wikipedia.WikipediaConstants;

public class RemoveWatchList extends VoltProcedure {
	
	public SQLStmt removeWatchList = new SQLStmt(
        "DELETE FROM " + WikipediaConstants.TABLENAME_WATCHLIST +
        " WHERE wl_user = ? AND wl_namespace = ? AND wl_title = ?"
    );
    public SQLStmt setUserTouched = new SQLStmt(
        "UPDATE " + WikipediaConstants.TABLENAME_USER +
        "   SET user_touched = ? " +
        " WHERE user_id =  ? "
    ); 

    public void run( int userId, int nameSpace, String pageTitle) {

        if (userId > 0) {
            voltQueueSQL(removeWatchList,1, userId);
            voltQueueSQL(removeWatchList,2, nameSpace);
            voltQueueSQL(removeWatchList,3, pageTitle);
            voltExecuteSQL();
            
            if (nameSpace == 0) {
                // if regular page, also remove a line of
                // watchlist for the corresponding talk page
                voltQueueSQL(removeWatchList,1, userId);
                voltQueueSQL(removeWatchList,2, 1);
                voltQueueSQL(removeWatchList,3, pageTitle);
                voltExecuteSQL();
                
            }
                        
            voltQueueSQL(setUserTouched, 1, new Timestamp().toString());
            voltQueueSQL(setUserTouched, 2, userId);
            voltExecuteSQL();
        }
    }
}
