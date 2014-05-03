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
// contestant and that the microwinhstorefull (phone number of the caller) is not above the
// number of allowed votes.
//

package edu.brown.benchmark.microwinhstorefull.procedures;

import org.voltdb.ProcInfo;
import org.voltdb.SQLStmt;
import org.voltdb.StmtInfo;
import org.voltdb.VoltProcedure;
import org.voltdb.VoltTable;
import org.voltdb.types.TimestampType;

import edu.brown.benchmark.microwinhstorefull.MicroWinHStoreFullConstants;

@ProcInfo (
	partitionInfo = "w_rows.phone_number:1",
    singlePartition = true
)
public class Vote extends VoltProcedure {
	   
    // Put the vote into the staging window
    public final SQLStmt insertVoteStagingStmt = new SQLStmt(
		"INSERT INTO w_staging (vote_id, phone_number, state, contestant_number, created) VALUES (?, ?, ?, ?, ?);"
    );
    
    @StmtInfo(
            upsertable=true
        )
    public final SQLStmt updateStagingCount = new SQLStmt(
    	"INSERT INTO staging_count (row_id, cnt) SELECT row_id, cnt + 1 FROM staging_count WHERE row_id = 1;"
    );
    
    @StmtInfo(
            upsertable=true
        )
    public final SQLStmt updateTotalVoteCount = new SQLStmt(
    	"INSERT INTO window_count (row_id, cnt) SELECT row_id, cnt + 1 FROM window_count WHERE row_id = 1;"
    );
    
 // Find the cutoff vote
    public final SQLStmt checkStagingCount = new SQLStmt(
		"SELECT cnt FROM staging_count WHERE row_id = 1;"
    );
    
 // Find the cutoff vote
    public final SQLStmt selectTotalVoteCountStmt = new SQLStmt(
		"SELECT cnt FROM window_count WHERE row_id = 1;"
    );

 // Find the cutoff vote
    public final SQLStmt selectFullWindowStmt = new SQLStmt(
		"SELECT vote_id FROM w_rows ORDER BY vote_id;"
    );
    
    // Find the cutoff vote
    public final SQLStmt selectCutoffVoteStmt = new SQLStmt(
		"SELECT vote_id FROM w_rows ORDER BY vote_id ASC LIMIT ?;"
    );
    
 // Find the cutoff vote
    public final SQLStmt deleteCutoffVoteStmt = new SQLStmt(
		"DELETE FROM w_rows WHERE vote_id <= ?;"
    );
    
    // Put the staging votes into the window
    public final SQLStmt insertVoteWindowStmt = new SQLStmt(
		"INSERT INTO w_rows (vote_id, phone_number, state, contestant_number, created) SELECT * FROM w_staging;"
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
    
    public final SQLStmt resetStagingCount = new SQLStmt(
    	"UPDATE staging_count SET cnt = 0 WHERE row_id = 1;"
    );
	
    public long run(long voteId, long phoneNumber, int contestantNumber, long maxVotesPerPhoneNumber) {
		
         // Post the vote
        TimestampType timestamp = new TimestampType();
        voltQueueSQL(insertVoteStagingStmt, voteId, phoneNumber, "XX", contestantNumber, timestamp);
        voltQueueSQL(updateStagingCount);
        voltQueueSQL(updateTotalVoteCount);
        voltQueueSQL(selectTotalVoteCountStmt);
        voltQueueSQL(checkStagingCount);
        VoltTable validation[] = voltExecuteSQL();
        
        int winsize = (int)MicroWinHStoreFullConstants.WINDOW_SIZE;
    	int slidesize = (int)MicroWinHStoreFullConstants.SLIDE_SIZE;
    	
    	int curwincount = (int)(validation[3].fetchRow(0).getLong(0));
    	int curstagecount = (int)(validation[4].fetchRow(0).getLong(0));
        
        if(curstagecount >= slidesize)
        {
        	
        	//Check the window size and cutoff vote can be done one of two ways:
        	//1) Two statements: one gets window size, one gets all rows to be deleted
        	//2) Return full window to Java, and let it sort it out.  Better for large slides.
        	//Likewise, either of these methods can be called in the earlier batch if that's better.
        	//voltQueueSQL(selectFullWindowStmt);
        	if(curwincount >= (winsize+slidesize))
        	{
        		voltQueueSQL(selectCutoffVoteStmt, slidesize);
        		validation = voltExecuteSQL();
        	
        		long cutoffVote = validation[0].fetchRow(slidesize-1).getLong(0);
        	
        		voltQueueSQL(deleteCutoffVoteStmt, cutoffVote);
        	}
        	
        	voltQueueSQL(insertVoteWindowStmt);
    		voltQueueSQL(deleteLeaderBoardStmt);
    		voltQueueSQL(updateLeaderBoardStmt);
        	
    		voltQueueSQL(deleteStagingStmt);
    		voltQueueSQL(resetStagingCount);
        }
        voltQueueSQL(selectLeaderBoardStmt);
        voltExecuteSQL(true);
		
        // Set the return value to 0: successful vote
        return MicroWinHStoreFullConstants.VOTE_SUCCESSFUL;
    }
}