/* This file is part of VoltDB.
 * Copyright (C) 2008-2012 VoltDB Inc.
 *
 * Permission is hereby granted, free of charge, to any person obtaining
 * a copy of this software and associated documentation files (the
 * "Software"), to deal in the Software without restriction, including
 * without limitation the rights to use, copy, modify, merge, publish,
 * distribute, sublicense, and/or sell copies of the Software, and to
 * permit persons to whom the Software is furnished to do so, subject to
 * the following conditions:
 *
 * The above copyright notice and this permission notice shall be
 * included in all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
 * IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR
 * OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
 * ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
 * OTHER DEALINGS IN THE SOFTWARE.
 */

//
// Accepts a vote, enforcing business logic: make sure the vote is for a valid
// contestant and that the voter (phone number of the caller) is not above the
// number of allowed votes.
//

package edu.brown.benchmark.microexperiments.nowindows.w100s2.procedures;

import org.voltdb.ProcInfo;
import org.voltdb.SQLStmt;
import org.voltdb.VoltProcedure;
import org.voltdb.VoltTable;
import org.voltdb.types.TimestampType;

import edu.brown.benchmark.microexperiments.nowindows.w100s2.NoWindowsConstants;

@ProcInfo (
    //partitionInfo = "a_tbl.a_id:1",
    singlePartition = true
)
public class ProcOne extends VoltProcedure {
	
    // Checks if the vote is for a valid contestant
    public final SQLStmt insertStmt = new SQLStmt(
	   "INSERT INTO a_staging VALUES (?,?,?);"
    );
    
    // Find the current window id
    public final SQLStmt checkState = new SQLStmt(
		"SELECT cnt, current_win_id FROM state_tbl WHERE row_id = 1;"
    );
    
    public final SQLStmt deleteCutoffTupleStmt = new SQLStmt(
		"DELETE FROM a_win WHERE win_id <= ?;"
    );
    
    public final SQLStmt insertTupleWindowStmt = new SQLStmt(
		"INSERT INTO a_win SELECT * FROM a_staging;"
    );
    
    public final SQLStmt deleteStagingStmt = new SQLStmt(
		"DELETE FROM a_staging;"
    );
    
    public final SQLStmt updateStagingCount = new SQLStmt(
		"UPDATE state_tbl SET cnt = ? WHERE row_id = 1;"
    );
    
    public final SQLStmt updateCurrentWinId = new SQLStmt(
		"UPDATE state_tbl SET current_win_id = ? WHERE row_id = 1;"
    );

	
    public long run(int row_id, int row_val) {
		
    	voltQueueSQL(checkState);
    	VoltTable validation[] = voltExecuteSQL(true);
       
        long stagingCount = validation[0].fetchRow(0).getLong(0) + 1;
        long currentWinId = validation[0].fetchRow(0).getLong(1) + 1;
        
        voltQueueSQL(insertStmt, currentWinId, row_id, row_val);
    	if(stagingCount == NoWindowsConstants.SLIDE_SIZE)
        {
        	//Check the window size and cutoff vote can be done one of two ways:
        	//1) Two statements: one gets window size, one gets all rows to be deleted
        	//2) Return full window to Java, and let it sort it out.  Better for large slides.
        	//Likewise, either of these methods can be called in the earlier batch if that's better.
        	
        	long cutoffId = currentWinId - NoWindowsConstants.WINDOW_SIZE;
            voltQueueSQL(deleteCutoffTupleStmt, cutoffId);
        	voltQueueSQL(insertTupleWindowStmt);
    		voltQueueSQL(deleteStagingStmt);
        }
    	voltQueueSQL(updateStagingCount, stagingCount % NoWindowsConstants.SLIDE_SIZE);
        voltQueueSQL(updateCurrentWinId, currentWinId);
        
        voltExecuteSQL(true);

        // Set the return value to 0: successful vote
        return NoWindowsConstants.PROC_ONE_SUCCESSFUL;
    }
}