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
// contestant and that the voterwintimehstore (phone number of the caller) is not above the
// number of allowed votes.
//

package edu.brown.benchmark.voterwintimehstore.procedures;

import org.voltdb.ProcInfo;
import org.voltdb.SQLStmt;
import org.voltdb.VoltProcedure;
import org.voltdb.VoltTable;
import org.voltdb.types.TimestampType;

import edu.brown.benchmark.voterwintimehstore.VoterWinTimeHStoreConstants;

@ProcInfo (
    partitionInfo = "votes.phone_number:1",
    singlePartition = true
)
public class Vote extends VoltProcedure {
	public final int windowSize = 100;
	public final int slideSize = 10;
	public final int tuplesPerTimestamp = 3;
	int currentTimestamp = 0;
	int tsCounter = 1;
	
    // Checks if the vote is for a valid contestant
    public final SQLStmt checkContestantStmt = new SQLStmt(
	   "SELECT contestant_number FROM contestants WHERE contestant_number = ?;"
    );
	
    // Checks if the voterwintimehstore has exceeded their allowed number of votes
    public final SQLStmt checkVoterWinTimeHStoreStmt = new SQLStmt(
		"SELECT num_votes FROM v_votes_by_phone_number WHERE phone_number = ?;"
    );
	
    // Checks an area code to retrieve the corresponding state
    public final SQLStmt checkStateStmt = new SQLStmt(
		"SELECT state FROM area_code_state WHERE area_code = ?;"
    );
	
    // Records a vote
    public final SQLStmt insertVoteStmt = new SQLStmt(
		"INSERT INTO votes (vote_id, phone_number, state, contestant_number, time) VALUES (?, ?, ?, ?, ?);"
    );
    
    // Put the vote into the staging window
    public final SQLStmt insertVoteStagingStmt = new SQLStmt(
		"INSERT INTO w_staging (vote_id, phone_number, state, contestant_number, time) VALUES (?, ?, ?, ?, ?);"
    );
    
 // Find the cutoff vote
    public final SQLStmt checkStagingCount = new SQLStmt(
		"SELECT min(time) FROM w_staging;"
    );
    
 // Find the cutoff vote
    public final SQLStmt selectWindowCountStmt = new SQLStmt(
		"SELECT count(*) FROM w_rows;"
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
		"INSERT INTO w_rows (vote_id, phone_number, state, contestant_number, time) SELECT * FROM w_staging;"
    );
    
 // Pull aggregate from window
    public final SQLStmt deleteLeaderBoardStmt = new SQLStmt(
		"DELETE FROM leaderboard;"
    );
    
    // Pull aggregate from window
    public final SQLStmt updateLeaderBoardStmt = new SQLStmt(
		"INSERT INTO leaderboard (contestant_number, numvotes) SELECT contestant_number, count(*) FROM w_rows GROUP BY contestant_number;"
    );
    
 // Clear the staging window
    public final SQLStmt deleteStagingStmt = new SQLStmt(
		"DELETE FROM w_staging;"
    );
    
 // Put the vote into the staging window
    public final SQLStmt UpdateLeaderBoardStmt = new SQLStmt(
		"INSERT INTO votes (vote_id, phone_number, state, contestant_number, created) VALUES (?, ?, ?, ?, ?);"
    );
	
    public long run(long voteId, long phoneNumber, int contestantNumber, long maxVotesPerPhoneNumber) {
		
        // Queue up validation statements
        voltQueueSQL(checkContestantStmt, contestantNumber);
        voltQueueSQL(checkVoterWinTimeHStoreStmt, phoneNumber);
        voltQueueSQL(checkStateStmt, (short)(phoneNumber / 10000000l));
        VoltTable validation[] = voltExecuteSQL();
		
        if (validation[0].getRowCount() == 0) {
            return VoterWinTimeHStoreConstants.ERR_INVALID_CONTESTANT;
        }
		
        if ((validation[1].getRowCount() == 1) &&
			(validation[1].asScalarLong() >= maxVotesPerPhoneNumber)) {
            return VoterWinTimeHStoreConstants.ERR_VOTER_OVER_VOTE_LIMIT;
        }
		
        // Some sample client libraries use the legacy random phone generation that mostly
        // created invalid phone numbers. Until refactoring, re-assign all such votes to
        // the "XX" fake state (those votes will not appear on the Live Statistics dashboard,
        // but are tracked as legitimate instead of invalid, as old clients would mostly get
        // it wrong and see all their transactions rejected).
        final String state = (validation[2].getRowCount() > 0) ? validation[2].fetchRow(0).getString(0) : "XX";
		 		
        // Post the vote
        TimestampType timestamp = new TimestampType();
        voltQueueSQL(insertVoteStmt, voteId, phoneNumber, state, contestantNumber, timestamp);
        voltQueueSQL(insertVoteStagingStmt, voteId, phoneNumber, state, contestantNumber, timestamp);
        voltQueueSQL(checkStagingCount);
        voltQueueSQL(selectWindowCountStmt);
        voltQueueSQL(selectCutoffVoteStmt, slideSize);
        validation = voltExecuteSQL();
        
        if((int)(validation[2].fetchRow(0).getLong(0)) == slideSize)
        {
        	//Check the window size and cutoff vote can be done one of two ways:
        	//1) Two statements: one gets window size, one gets all rows to be deleted
        	//2) Return full window to Java, and let it sort it out.  Better for large slides.
        	//Likewise, either of these methods can be called in the earlier batch if that's better.
        	//voltQueueSQL(selectFullWindowStmt);
        	
        	//validation = voltExecuteSQL();
        	if((int)(validation[3].fetchRow(0).getLong(0)) >= windowSize)
        	{
        		long cutoffId = validation[4].fetchRow(slideSize-1).getLong(0);
        		voltQueueSQL(deleteCutoffVoteStmt, cutoffId);
        	}
        	voltQueueSQL(insertVoteWindowStmt);
    		voltQueueSQL(deleteLeaderBoardStmt);
    		voltQueueSQL(updateLeaderBoardStmt);
    		voltQueueSQL(deleteStagingStmt);
    		voltExecuteSQL(true);
        }
		
        // Set the return value to 0: successful vote
        return VoterWinTimeHStoreConstants.VOTE_SUCCESSFUL;
    }
}