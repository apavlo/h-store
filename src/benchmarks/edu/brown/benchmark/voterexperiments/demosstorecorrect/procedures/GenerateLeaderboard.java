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

package edu.brown.benchmark.voterexperiments.demosstorecorrect.procedures;

import org.voltdb.ProcInfo;
import org.voltdb.SQLStmt;
import org.voltdb.StmtInfo;
import org.voltdb.VoltProcedure;
import org.voltdb.VoltTable;
import org.voltdb.types.TimestampType;

import edu.brown.benchmark.voterexperiments.demohstorecorrect.VoterDemoHStoreConstants;
import edu.brown.benchmark.voterexperiments.demosstorecorrect.VoterDemoSStoreConstants;

@ProcInfo (
	//partitionInfo = "contestants.contestant_number:1",
    singlePartition = true
)
public class GenerateLeaderboard extends VoltProcedure {
	
	protected void toSetTriggerTableName()
	{
		addTriggerTable("proc_one_out");
	}
	
	// Put vote into leaderboard
    public final SQLStmt trendingLeaderboardStmt = new SQLStmt(
	   "INSERT INTO trending_leaderboard (vote_id, phone_number, state, contestant_number, created) SELECT * FROM proc_one_out;"
    );
	
	public final SQLStmt deleteProcOneOutStmt = new SQLStmt(
		"DELETE FROM proc_one_out;"
	);
	    
    public final SQLStmt checkNumVotesStmt = new SQLStmt(
		"SELECT cnt FROM votes_count WHERE row_id = 1;"
    );
    
    public final SQLStmt updateNumVotesStmt = new SQLStmt(
		"UPDATE votes_count SET cnt = ? WHERE row_id = 1;"
    );
    
    public final SQLStmt insertProcTwoOutStmt = new SQLStmt(
    	"INSERT INTO proc_two_out VALUES (?);"	
    );
    
	/////////////////////////////
	//BEGIN DEMO BOARD UPDATES
	/////////////////////////////
	public final SQLStmt deleteDemoTopBoard = new SQLStmt(
	"DELETE FROM demoTopBoard;");
	
	public final SQLStmt deleteDemoTrendingBoard = new SQLStmt(
	"DELETE FROM demoTrendingBoard;");
	
	public final SQLStmt deleteDemoVoteCount = new SQLStmt(
	"DELETE FROM demoVoteCount;");
	
	public final SQLStmt deleteDemoWindowCount = new SQLStmt(
	"DELETE FROM demoWindowCount;");
	
	public final SQLStmt updateDemoTopBoard = new SQLStmt(
	"INSERT INTO demoTopBoard "
	+ " SELECT a.contestant_name   AS contestant_name"
	+ "         , a.contestant_number AS contestant_number"
	+ "        , b.num_votes          AS num_votes"
	+ "     FROM v_votes_by_contestant b"
	+ "        , contestants AS a"
	+ "    WHERE a.contestant_number = b.contestant_number");
	
	public final SQLStmt updateDemoTrendingBoard = new SQLStmt( "INSERT INTO demoTrendingBoard "
	+ "   SELECT a.contestant_name   AS contestant_name"
	+ "        , a.contestant_number AS contestant_number"
	+ "        , b.num_votes          AS num_votes"
	+ "     FROM leaderboard b"
	+ "        , contestants AS a"
	+ "    WHERE a.contestant_number = b.contestant_number");
	
	public final SQLStmt updateDemoVoteCount = new SQLStmt( "INSERT INTO demoVoteCount "
	+ "SELECT count(*) FROM votes;");
	
	public final SQLStmt updateDemoWindowCount = new SQLStmt( "INSERT INTO demoWindowCount "
	+ "SELECT count(*) FROM trending_leaderboard;");
	
	public final SQLStmt checkDemo = new SQLStmt( "INSERT INTO demoWindowCount VALUES (-1);");
	/////////////////////////////
	//END DEMO BOARD UPDATES
	/////////////////////////////
	
    public long run() {
		
        voltQueueSQL(trendingLeaderboardStmt);
        voltQueueSQL(deleteProcOneOutStmt);
        voltQueueSQL(checkNumVotesStmt);
        
        VoltTable validation[] = voltExecuteSQL();

        int numVotes = (int)(validation[2].fetchRow(0).getLong(0)) + 1;
        
        voltQueueSQL(updateNumVotesStmt, (numVotes % VoterDemoSStoreConstants.VOTE_THRESHOLD));
        
        // Set the return value to 0: successful vote
        if(((int)numVotes % (int)VoterDemoSStoreConstants.BOARD_REFRESH) == 0)
        {
        	voltQueueSQL(deleteDemoTopBoard);
        	voltQueueSQL(deleteDemoTrendingBoard);
        	voltQueueSQL(deleteDemoVoteCount);
        	voltQueueSQL(deleteDemoWindowCount);
        	voltQueueSQL(updateDemoTopBoard);
        	voltQueueSQL(updateDemoTrendingBoard);
        	voltQueueSQL(updateDemoVoteCount);
        	voltQueueSQL(updateDemoWindowCount);
        }
        
        // Set the return value to 0: successful vote
        if(numVotes == VoterDemoSStoreConstants.VOTE_THRESHOLD)
        {
        	voltQueueSQL(insertProcTwoOutStmt, 1);
        }
        voltExecuteSQL(true);
        
        return VoterDemoSStoreConstants.WINDOW_SUCCESSFUL;
    }
}