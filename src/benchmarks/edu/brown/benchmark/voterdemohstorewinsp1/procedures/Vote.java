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
// contestant and that the voterdemohstorewinsp1 (phone number of the caller) is not above the
// number of allowed votes.
//

package edu.brown.benchmark.voterdemohstorewinsp1.procedures;

import org.voltdb.ProcInfo;
import org.voltdb.SQLStmt;
import org.voltdb.StmtInfo;
import org.voltdb.VoltProcedure;
import org.voltdb.VoltTable;
import org.voltdb.types.TimestampType;

import edu.brown.benchmark.voter.VoterConstants;
import edu.brown.benchmark.voterdemohstorewinsp1.VoterDemoHStoreWinSP1Constants;
import edu.brown.benchmark.voterwintimehstore.VoterWinTimeHStoreConstants;

@ProcInfo (
    partitionInfo = "votes.phone_number:1",
    singlePartition = true
)
public class Vote extends VoltProcedure {
	
	@StmtInfo(
            upsertable=true
        )
    public final SQLStmt updateTotalCount = new SQLStmt(
    	"INSERT INTO totalVoteCount (row_id, cnt) SELECT row_id, cnt + 1 FROM totalVoteCount WHERE row_id = 1;"
    );

    // Checks if the vote is for a valid contestant
    public final SQLStmt checkContestantStmt = new SQLStmt(
	   "SELECT contestant_number FROM contestants WHERE contestant_number = ?;"
    );
	
    // Checks if the voter has exceeded their allowed number of votes
    public final SQLStmt checkVoterStmt = new SQLStmt(
		"SELECT num_votes FROM v_votes_by_phone_number WHERE phone_number = ?;"
    );
	
    // Checks an area code to retrieve the corresponding state
    public final SQLStmt checkStateStmt = new SQLStmt(
		"SELECT state FROM area_code_state WHERE area_code = ?;"
    );
	
    // Records a vote
    public final SQLStmt insertVoteStmt = new SQLStmt(
		"INSERT INTO votes (vote_id, phone_number, state, contestant_number, ts) VALUES (?, ?, ?, ?, ?);"
    );
    
    // Records a vote
    public final SQLStmt insertProcEndStmt = new SQLStmt(
		"INSERT INTO proc_one_out (vote_id, phone_number, state, contestant_number, ts) VALUES (?, ?, ?, ?, ?);"
    );
    
    public final SQLStmt trendingLeaderboardStmt = new SQLStmt(
	   "INSERT INTO w_staging (vote_id, phone_number, state, contestant_number, ts) SELECT * FROM proc_one_out;"
    );
    
    public final SQLStmt clearProcOut = new SQLStmt(
    	"DELETE FROM proc_one_out;"	
    );
    
    /**
    public final SQLStmt updateCountingWin = new SQLStmt(
    	"INSERT INTO counting_win (vote_id) SELECT vote_id FROM proc_one_out;"	
    );
    */
    
    /**
    public final SQLStmt checkStagingTimestamp = new SQLStmt(
    	"SELECT MAX(ts) FROM w_staging;"	
    );
    
    public final SQLStmt checkWindowTimestamp = new SQLStmt(
    	"SELECT MIN(ts) FROM w_trending_leaderboard;"	
    );*/
    
    public final SQLStmt checkWindowTimestamp = new SQLStmt(
    	"SELECT minTS FROM minWindow WHERE row_id = 1;"	
    );
    
    public final SQLStmt updateWindowTS = new SQLStmt(
        "UPDATE minWindow SET minTS = ? WHERE row_id = 1;"
    );
    
    @StmtInfo(
            upsertable=true
        )
    public final SQLStmt updateCount = new SQLStmt(
    	"INSERT INTO voteCount (row_id, cnt) SELECT row_id, cnt + 1 FROM voteCount WHERE row_id = 1;"
    );
    
    public final SQLStmt getCount = new SQLStmt(
    	"SELECT cnt FROM voteCount;"
    );
    
    /**
    //hack to avoid views
    public final SQLStmt clearLowestContestant = new SQLStmt(
    	"DELETE FROM votes_by_contestant;"
    );
    
  //hack to avoid views
    public final SQLStmt setLowestContestants = new SQLStmt(
    	//"SELECT contestant_number, num_votes FROM v_contestant_count ORDER BY num_votes ASC LIMIT 1;"
    	"INSERT INTO votes_by_contestant (contestant_number,num_votes) SELECT contestant_number, count(*) FROM votes GROUP BY contestant_number;"
    );
    */
    
	////////////////////////////second batch of SQL statements/////////////////////////////////////////   
	public final SQLStmt slideWindow1 = new SQLStmt(
	"DELETE FROM w_trending_leaderboard WHERE ts < ?;"
	);
	
	public final SQLStmt slideWindow2 = new SQLStmt(
	"INSERT INTO w_trending_leaderboard (vote_id, phone_number, state, contestant_number, ts) SELECT * FROM w_staging;"
	);
	
	public final SQLStmt clearStaging = new SQLStmt(
	"DELETE FROM w_staging;"
	);
	
	public final SQLStmt deleteLeaderboard = new SQLStmt(
	"DELETE FROM top_three_last_30_sec;"
	);
	
	public final SQLStmt updateLeaderboard = new SQLStmt(
	"INSERT INTO top_three_last_30_sec (contestant_number, num_votes) SELECT contestant_number, count(*) FROM w_trending_leaderboard GROUP BY contestant_number;"
	);
	
	 public final SQLStmt resetCount = new SQLStmt(
    	"UPDATE voteCount SET cnt = 0;"	
    );
	 
	 public final SQLStmt getTopLeaderboard = new SQLStmt(
    	"SELECT contestant_number, num_votes FROM v_votes_by_contestant ORDER BY num_votes DESC LIMIT 3;"	
    );
    
    public final SQLStmt getBottomLeaderboard = new SQLStmt(
    	"SELECT contestant_number, num_votes FROM v_votes_by_contestant ORDER BY num_votes ASC LIMIT 3;"	
    );
    
    public final SQLStmt getTrendingLeaderboard = new SQLStmt(
    	"SELECT contestant_number, num_votes FROM v_top_three_last_30_sec ORDER BY num_votes DESC LIMIT 3;"	
    );
    
	
public long run(long voteId, long phoneNumber, int contestantNumber, long maxVotesPerPhoneNumber, int currentTimestamp) {
		
        // Queue up validation statements
		voltQueueSQL(checkContestantStmt, contestantNumber);
        voltQueueSQL(checkVoterStmt, phoneNumber);
        voltQueueSQL(checkStateStmt, (short)(phoneNumber / 10000000l));
        voltQueueSQL(updateTotalCount);
        VoltTable validation[] = voltExecuteSQL();
		
        // validate the maximum limit for votes number
        if (validation[0].getRowCount() == 0) {
            return VoterConstants.ERR_INVALID_CONTESTANT;
        }
        
        if ((validation[1].getRowCount() == 1) &&
			(validation[1].fetchRow(0).getLong(0) >= maxVotesPerPhoneNumber)) {
            return VoterConstants.ERR_VOTER_OVER_VOTE_LIMIT;
        }
		
        // Some sample client libraries use the legacy random phone generation that mostly
        // created invalid phone numbers. Until refactoring, re-assign all such votes to
        // the "XX" fake state (those votes will not appear on the Live Statistics dashboard,
        // but are tracked as legitimate instead of invalid, as old clients would mostly get
        // it wrong and see all their transactions rejected).
        final String state = (validation[2].getRowCount() > 0) ? validation[2].fetchRow(0).getString(0) : "XX";
		 	
        
        // Post the vote
        //TimestampType timestamp = new TimestampType();
        voltQueueSQL(insertVoteStmt, voteId, phoneNumber, state, contestantNumber, currentTimestamp);
      	voltQueueSQL(checkWindowTimestamp);
        voltQueueSQL(updateCount);
        voltQueueSQL(getCount);
        voltQueueSQL(trendingLeaderboardStmt);
        validation = voltExecuteSQL();
        
        //long currentTimestamp = validation[0].fetchRow(0).getLong(0);
        int minWinTimestamp = (int)validation[1].fetchRow(0).getLong(0);
        int voteCount = (int)validation[3].fetchRow(0).getLong(0);
        
        if(currentTimestamp - minWinTimestamp >= VoterDemoHStoreWinSP1Constants.WINDOW_SIZE + VoterDemoHStoreWinSP1Constants.SLIDE_SIZE)
        {
        	voltQueueSQL(updateWindowTS, currentTimestamp - VoterDemoHStoreWinSP1Constants.WINDOW_SIZE);
        	voltQueueSQL(slideWindow1, currentTimestamp - VoterDemoHStoreWinSP1Constants.WINDOW_SIZE);
        	voltQueueSQL(slideWindow2);
        	voltQueueSQL(clearStaging);
        	voltQueueSQL(deleteLeaderboard);
        	voltQueueSQL(updateLeaderboard);
        }
        voltQueueSQL(getTrendingLeaderboard);
        voltQueueSQL(getTopLeaderboard);
        voltQueueSQL(getBottomLeaderboard);
        voltExecuteSQL();
        
        if(voteCount >= VoterDemoHStoreWinSP1Constants.VOTE_THRESHOLD)
        {
        	voltQueueSQL(resetCount);
        	voltExecuteSQL(true);
        	return VoterDemoHStoreWinSP1Constants.VOTE_SUCCESSFUL_TRIG_SP2;
        }
        
        // Set the return value to 0: successful vote
        return VoterDemoHStoreWinSP1Constants.VOTE_SUCCESSFUL;
    }
}