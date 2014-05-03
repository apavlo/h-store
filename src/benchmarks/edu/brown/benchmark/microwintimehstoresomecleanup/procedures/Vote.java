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
// contestant and that the microwintimehstoresomecleanup (phone number of the caller) is not above the
// number of allowed votes.
//

package edu.brown.benchmark.microwintimehstoresomecleanup.procedures;

import org.voltdb.ProcInfo;
import org.voltdb.SQLStmt;
import org.voltdb.StmtInfo;
import org.voltdb.VoltProcedure;
import org.voltdb.VoltTable;
import org.voltdb.types.TimestampType;

import edu.brown.benchmark.microwintimehstoresomecleanup.MicroWinTimeHStoreSomeCleanupConstants;

@ProcInfo (
	partitionInfo = "w_rows.phone_number:1",
    singlePartition = true
)
public class Vote extends VoltProcedure {
	   
    // Put the vote into the staging window
    public final SQLStmt insertVoteStagingStmt = new SQLStmt(
		"INSERT INTO w_staging (vote_id, phone_number, state, contestant_number, ts) VALUES (?, ?, ?, ?, ?);"
    );
    
 // Find the cutoff vote
    public final SQLStmt checkMinWindowTimestamp = new SQLStmt(
		"SELECT ts FROM min_window WHERE row_id = 1;"
    );
    
    public final SQLStmt updateMinWindowTS = new SQLStmt(
    	"UPDATE min_window SET ts = ? WHERE row_id = 1;"
    );
    
    // Find the cutoff vote
    public final SQLStmt checkMinStagingTimestamp = new SQLStmt(
		"SELECT ts FROM min_staging WHERE row_id = 1;"
    );
    
    public final SQLStmt updateMinStagingTS = new SQLStmt(
    	"UPDATE min_staging SET ts = ? WHERE row_id = 1;"
    );
    
    public final SQLStmt removeExpiredWinVotes = new SQLStmt(
    	"DELETE FROM w_rows WHERE ts < ?;"	
    );
    
    // Put the staging votes into the window
    public final SQLStmt insertVoteWindowStmt = new SQLStmt(
		"INSERT INTO w_rows (vote_id, phone_number, state, contestant_number, ts) SELECT * FROM w_staging;"
    );
    
 // Pull aggregate from window
    public final SQLStmt deleteLeaderBoardStmt = new SQLStmt(
		"DELETE FROM leaderboard;"
    );
    
    // Pull aggregate from window
    public final SQLStmt updateLeaderBoardStmt = new SQLStmt(
		"INSERT INTO leaderboard (contestant_number, numvotes) SELECT contestant_number, count(*) FROM w_rows GROUP BY contestant_number;"
    );
    
    public final SQLStmt selectLeaderBoardStmt = new SQLStmt(
    	"SELECT contestant_number, numvotes FROM leaderboard ORDER BY numvotes DESC;"
    );
    
 // Clear the staging window
    public final SQLStmt deleteStagingStmt = new SQLStmt(
		"DELETE FROM w_staging;"
    );

    public long run(long voteId, long phoneNumber, int contestantNumber, long maxVotesPerPhoneNumber, int timestamp) {
		
        voltQueueSQL(checkMinWindowTimestamp);
        voltQueueSQL(checkMinStagingTimestamp);
        VoltTable validation[] = voltExecuteSQL();
        
        int winsize = (int)MicroWinTimeHStoreSomeCleanupConstants.WINDOW_SIZE;
    	int slidesize = (int)MicroWinTimeHStoreSomeCleanupConstants.SLIDE_SIZE;
    	
    	int curminwindow = (int)(validation[0].fetchRow(0).getLong(0));
    	int curminstaging = (int)(validation[1].fetchRow(0).getLong(0));
        
        if(timestamp - curminstaging >= slidesize)
        {
        	voltQueueSQL(removeExpiredWinVotes, timestamp - winsize);
        	voltQueueSQL(insertVoteWindowStmt);
        	voltQueueSQL(deleteLeaderBoardStmt);
    		voltQueueSQL(updateLeaderBoardStmt);
    		voltQueueSQL(updateMinWindowTS, timestamp - winsize);
    		voltQueueSQL(updateMinStagingTS, timestamp);
    		voltQueueSQL(deleteStagingStmt);
        }
        voltQueueSQL(insertVoteStagingStmt, voteId, phoneNumber, "XX", contestantNumber, timestamp);
        voltQueueSQL(selectLeaderBoardStmt);
        voltExecuteSQL(true);
		
        // Set the return value to 0: successful vote
        return MicroWinTimeHStoreSomeCleanupConstants.VOTE_SUCCESSFUL;
    }
}