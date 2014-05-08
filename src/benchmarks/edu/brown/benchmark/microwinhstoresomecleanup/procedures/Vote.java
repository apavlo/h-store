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
// contestant and that the microwinhstorecleanup (phone number of the caller) is not above the
// number of allowed votes.
//

package edu.brown.benchmark.microwinhstoresomecleanup.procedures;

import java.util.Random;

import org.voltdb.ProcInfo;
import org.voltdb.SQLStmt;
import org.voltdb.StmtInfo;
import org.voltdb.VoltProcedure;
import org.voltdb.VoltTable;
import org.voltdb.VoltTableRow;
import org.voltdb.types.TimestampType;

import edu.brown.benchmark.microwinhstoresomecleanup.MicroWinHStoreSomeCleanupConstants;

@ProcInfo (
	partitionInfo = "w_rows.phone_number:1",
    singlePartition = true
)
public class Vote extends VoltProcedure {
	private final Random rand = new Random();
	   
    // Put the vote into the staging window
    public final SQLStmt insertVoteStmt = new SQLStmt(
		"INSERT INTO w_rows (vote_id, phone_number, state, contestant_number, created) VALUES (?, ?, ?, ?, ?);"
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
    
    public final SQLStmt getMaxWinIdStmt = new SQLStmt(
        	"SELECT vote_id FROM max_win_id WHERE row_id = 1;"
        );
       
    public final SQLStmt updateMaxWinIdStmt = new SQLStmt(
    	"UPDATE max_win_id SET vote_id = ? WHERE row_id = 1;"
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
		"SELECT vote_id FROM w_rows ORDER BY vote_id DESC LIMIT ?;"
    );
    
 // Find the cutoff vote
    public final SQLStmt deleteCutoffVoteStmt = new SQLStmt(
		"DELETE FROM w_rows WHERE vote_id < ?;"
    );
    
 // Pull aggregate from window
    public final SQLStmt deleteLeaderBoardStmt = new SQLStmt(
		"DELETE FROM leaderboard;"
    );
    
    // Pull aggregate from window
    public final SQLStmt selectWindowStmt = new SQLStmt(
		"SELECT contestant_number, count(*) FROM w_rows WHERE vote_id >= ? GROUP BY contestant_number;"
    );
    
 // Pull aggregate from window
    public final SQLStmt insertLeaderBoardStmt = new SQLStmt(
		"INSERT INTO leaderboard (contestant_number, numvotes) VALUES (?,?);"
    );
    
    public final SQLStmt selectLeaderBoardStmt = new SQLStmt(
    	"SELECT contestant_number, numvotes FROM leaderboard ORDER BY numvotes DESC;"
    );
    
    
    public final SQLStmt resetStagingCount = new SQLStmt(
    	"UPDATE staging_count SET cnt = 0 WHERE row_id = 1;"
    );
	
    public long run(long voteId, long phoneNumber, int contestantNumber, long maxVotesPerPhoneNumber) {
		
         // Post the vote
        TimestampType timestamp = new TimestampType();
        voltQueueSQL(insertVoteStmt, voteId, phoneNumber, "XX", contestantNumber, timestamp);
        voltQueueSQL(updateStagingCount);
        voltQueueSQL(updateTotalVoteCount);
        voltQueueSQL(checkStagingCount);
        voltQueueSQL(selectTotalVoteCountStmt);
        VoltTable validation[] = voltExecuteSQL();
        
        int winsize = (int)MicroWinHStoreSomeCleanupConstants.WINDOW_SIZE;
    	int slidesize = (int)MicroWinHStoreSomeCleanupConstants.SLIDE_SIZE;
    	
    	
    	int curstagecount = (int)(validation[3].fetchRow(0).getLong(0));
    	int curwincount = (int)(validation[4].fetchRow(0).getLong(0));
    	long cutoffVote = 0;
        
        if(curstagecount >= slidesize)
        {      		
        	//Check the window size and cutoff vote can be done one of two ways:
        	//1) Two statements: one gets window size, one gets all rows to be deleted
        	//2) Return full window to Java, and let it sort it out.  Better for large slides.
        	//Likewise, either of these methods can be called in the earlier batch if that's better.
        	//voltQueueSQL(selectFullWindowStmt);
        	if(curwincount >= (winsize+slidesize))
        	{
        		voltQueueSQL(selectCutoffVoteStmt, winsize);
        		validation = voltExecuteSQL();
        	
        		cutoffVote = validation[0].fetchRow(winsize-1).getLong(0);
        		if(rand.nextInt(winsize/slidesize) == 0)
        		{
	        		voltQueueSQL(deleteCutoffVoteStmt, cutoffVote);
	        		voltExecuteSQL();
        		}
        	}
        	voltQueueSQL(selectWindowStmt, cutoffVote);
        	validation = voltExecuteSQL();
        	voltQueueSQL(deleteLeaderBoardStmt);
        	for(int i = 0; i < validation[0].getRowCount(); i++)
        	{
        		VoltTableRow row = validation[0].fetchRow(i);
        		voltQueueSQL(insertLeaderBoardStmt, row.getLong(0), row.getLong(1));
        	}
        	        	
        	voltQueueSQL(updateMaxWinIdStmt, voteId);
    		voltQueueSQL(resetStagingCount);
        }
        
        voltQueueSQL(selectLeaderBoardStmt);
        voltExecuteSQL(true);
		
        // Set the return value to 0: successful vote
        return MicroWinHStoreSomeCleanupConstants.VOTE_SUCCESSFUL;
    }
}