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
// contestant and that the voterdemosstore (phone number of the caller) is not above the
// number of allowed votes.
//

package edu.brown.benchmark.voterdemosstorepetrigonly.procedures;

import org.voltdb.ProcInfo;
import org.voltdb.SQLStmt;
import org.voltdb.StmtInfo;
import org.voltdb.VoltProcedure;
import org.voltdb.VoltTable;
import org.voltdb.types.TimestampType;

import edu.brown.benchmark.voterdemohstorenocleanup.VoterDemoHStoreNoCleanupConstants;
import edu.brown.benchmark.voterdemosstorepetrigonly.VoterDemoSStorePETrigOnlyConstants;

@ProcInfo (
	//partitionInfo = "contestants.contestant_number:1",
    singlePartition = true
)
public class GenerateLeaderboard extends VoltProcedure {
    
	protected void toSetTriggerTableName()
	{
		addTriggerTable("proc_one_out");
	}
    
    public final SQLStmt clearProcOut = new SQLStmt(
    	"DELETE FROM proc_one_out;"	
    );
	
	////////////////////
	public final SQLStmt checkWindowTimestampStmt = new SQLStmt(
	"SELECT ts FROM win_timestamp WHERE row_id = 1;" //min window ts
	);
	
	public final SQLStmt checkMaxStagingTimestampStmt = new SQLStmt(
	"SELECT ts FROM stage_timestamp WHERE row_id = 1;"
	);
	
	// Put the staging votes into the window
	public final SQLStmt selectVoteWindowStmt = new SQLStmt(
	"SELECT contestant_number, count(*) FROM votes WHERE ts >= ? AND ts < ? GROUP BY contestant_number;"
	);
	
	public final SQLStmt updateMinTSStmt = new SQLStmt(
	"UPDATE win_timestamp SET ts = ? WHERE row_id = 1;" //min window ts
	);
	////////////////////
	
	@StmtInfo(
	upsertable=true
	)
	public final SQLStmt updateCount = new SQLStmt(
	"INSERT INTO voteCount (row_id, cnt) SELECT row_id, cnt + 1 FROM voteCount WHERE row_id = 1;"
	);
	
	@StmtInfo(
	upsertable=true
	)
	public final SQLStmt updateTotalCount = new SQLStmt(
	"INSERT INTO totalLeaderboardCount (row_id, cnt) SELECT row_id, cnt + 1 FROM totalLeaderboardCount WHERE row_id = 1;"
	);
	
	public final SQLStmt getCount = new SQLStmt(
	"SELECT cnt FROM voteCount;"
	);
	
	public final SQLStmt getLowestContestant = new SQLStmt(
	"SELECT contestant_number, num_votes FROM v_votes_by_contestant ORDER BY num_votes ASC LIMIT 1;"
	);
	
	
	////////////////////////////third batch of SQL statements//////////////////////////////////////////
	public final SQLStmt deleteContestant = new SQLStmt(
	"DELETE FROM contestants WHERE contestant_number = ?;"	
	);
	
	public final SQLStmt deleteVotes = new SQLStmt(
	"DELETE FROM votes WHERE contestant_number = ?;"	
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
	
	
	public long run() {
	
	// Queue up leaderboard stmts
	voltQueueSQL(updateCount);
	voltQueueSQL(checkMaxStagingTimestampStmt);//1
	voltQueueSQL(checkWindowTimestampStmt);//2
	voltQueueSQL(getCount); //3
	voltQueueSQL(getLowestContestant); //4
	voltQueueSQL(getTopLeaderboard);
	voltQueueSQL(getBottomLeaderboard);
	voltQueueSQL(updateTotalCount);
	voltQueueSQL(clearProcOut);
	
	VoltTable validation[] = voltExecuteSQL();
	
	long maxStageTimestamp = validation[1].fetchRow(0).getLong(0);
	long minWinTimestamp = validation[2].fetchRow(0).getLong(0);
	long voteCount = validation[3].fetchRow(0).getLong(0);
	long lowestContestant = validation[4].fetchRow(0).getLong(0);
	
	if(maxStageTimestamp - minWinTimestamp >= VoterDemoHStoreNoCleanupConstants.WIN_SIZE + VoterDemoHStoreNoCleanupConstants.STAGE_SIZE)
	{
	minWinTimestamp = maxStageTimestamp - VoterDemoHStoreNoCleanupConstants.WIN_SIZE;
	voltQueueSQL(selectVoteWindowStmt, minWinTimestamp, maxStageTimestamp);
	voltQueueSQL(updateMinTSStmt, minWinTimestamp);
	voltExecuteSQL();
	}
	
	// check the number of votes so far
	if ( voteCount >= VoterDemoHStoreNoCleanupConstants.VOTE_THRESHOLD) {
	long contestant_number = lowestContestant;
	
	voltQueueSQL(deleteVotes, contestant_number);
	voltQueueSQL(deleteContestant, contestant_number);
	//voltQueueSQL(getLowestContestant);
	voltQueueSQL(resetCount);
	voltQueueSQL(selectVoteWindowStmt, minWinTimestamp, minWinTimestamp + VoterDemoHStoreNoCleanupConstants.WIN_SIZE);
	voltQueueSQL(getTopLeaderboard);
	voltQueueSQL(getBottomLeaderboard);
	voltExecuteSQL(true);
	}
	
	// Set the return value to 0: successful vote
	return VoterDemoHStoreNoCleanupConstants.VOTE_SUCCESSFUL;
	}
}