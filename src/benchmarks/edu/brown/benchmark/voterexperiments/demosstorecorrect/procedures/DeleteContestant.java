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

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.io.ObjectOutputStream;

import org.voltdb.ProcInfo;
import org.voltdb.SQLStmt;
import org.voltdb.StmtInfo;
import org.voltdb.VoltProcedure;
import org.voltdb.VoltTable;
import org.voltdb.types.TimestampType;

import edu.brown.benchmark.voterexperiments.demosstorecorrect.VoterDemoSStoreConstants;
import edu.brown.benchmark.voterexperiments.demosstorecorrect.VoterDemoSStoreUtil;

@ProcInfo (
	//partitionInfo = "contestants.contestant_number:1",
    singlePartition = true
)
public class DeleteContestant extends VoltProcedure {
	
	protected void toSetTriggerTableName()
	{
		addTriggerTable("proc_two_out");
	}
    
	public final SQLStmt deleteProcTwoOutStmt = new SQLStmt(
		"DELETE FROM proc_two_out;"
	);
	
    public final SQLStmt findLowestContestant = new SQLStmt(
		"SELECT * FROM v_votes_by_contestant ORDER BY num_votes ASC LIMIT 1;"
    );
    
    public final SQLStmt deleteLowestContestant = new SQLStmt(
		"DELETE FROM contestants WHERE contestant_number = ?;"
    );
    
    public final SQLStmt deleteLowestVotes = new SQLStmt(
		"DELETE FROM votes WHERE contestant_number = ?;"
    );
    
 // Pull aggregate from window
    public final SQLStmt deleteLeaderBoardStmt = new SQLStmt(
		"DELETE FROM leaderboard WHERE contestant_number = ?;"
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
    
	/////////////////////////////
	//END DEMO BOARD UPDATES
	/////////////////////////////
    
	/////////////////////////////
	//BEGIN GET RESULTS
	/////////////////////////////
	public final SQLStmt getTopThreeVotesStmt = new SQLStmt( "   SELECT a.contestant_name   AS contestant_name"
		  + "        , b.num_votes          AS num_votes"
		  + "     FROM v_votes_by_contestant b"
		  + "        , contestants AS a"
		  + "    WHERE a.contestant_number = b.contestant_number"
		  + " ORDER BY num_votes DESC"
		  + "        , contestant_number ASC"
		  + " LIMIT 3");
	
	public final SQLStmt getBottomThreeVotesStmt = new SQLStmt( "   SELECT a.contestant_name   AS contestant_name"
		  + "        , b.num_votes          AS num_votes"
		  + "     FROM v_votes_by_contestant b"
		  + "        , contestants AS a"
		  + "    WHERE a.contestant_number = b.contestant_number"
		  + " ORDER BY num_votes ASC"
		  + "        , contestant_number ASC"
		  + " LIMIT 3");
	
	public final SQLStmt getTrendingStmt = new SQLStmt( "   SELECT a.contestant_name   AS contestant_name"
		  + "        , b.num_votes          AS num_votes"
		  + "     FROM leaderboard b"
		  + "        , contestants AS a"
		  + "    WHERE a.contestant_number = b.contestant_number"
		  + " ORDER BY num_votes DESC"
		  + "        , contestant_number ASC"
		  + " LIMIT 3");
	
	public final SQLStmt getAllVotesStmt = new SQLStmt( "   SELECT a.contestant_name   AS contestant_name"
			  + "        , b.num_votes          AS num_votes"
			  + "     FROM v_votes_by_contestant b"
			  + "        , contestants AS a"
			  + "    WHERE a.contestant_number = b.contestant_number"
			  + " ORDER BY num_votes ASC"
			  + "        , contestant_number ASC");
	
	public final SQLStmt getVoteCountStmt = new SQLStmt( "SELECT cnt FROM votes_count WHERE row_id=1;");
	public final SQLStmt getActualVoteCountStmt = new SQLStmt( "SELECT totalcnt, successcnt FROM proc_one_count WHERE row_id = 1;");
	public final SQLStmt getTrendingCountStmt = new SQLStmt("SELECT count(*) FROM trending_leaderboard;");
	/////////////////////////////
	//END GET RESULTS
	/////////////////////////////
	
	private void printResults() throws IOException
	{
		ArrayList<String> tableNames = new ArrayList<String>();
		if(!VoterDemoSStoreConstants.DEBUG)
		{
			voltQueueSQL(getTopThreeVotesStmt);
	        tableNames.add("TopThree");
	        voltQueueSQL(getBottomThreeVotesStmt);
	        tableNames.add("BottomThree");
	        voltQueueSQL(getTrendingStmt);
	        tableNames.add("TrendingThree");
	        voltQueueSQL(getVoteCountStmt);
			tableNames.add("VoteCount");
			voltQueueSQL(getTrendingCountStmt);
			tableNames.add("TrendingCount");
		}
		else {
	        voltQueueSQL(getAllVotesStmt);
	        tableNames.add("Votes");
	        voltQueueSQL(getActualVoteCountStmt);
			tableNames.add("ProcOneCounts");
		}
		
		VoltTable[] v = voltExecuteSQL();
		VoterDemoSStoreUtil.writeToFile(v, tableNames, VoterDemoSStoreConstants.DELETE_CODE);
	}
	
    public long run() {
		
        voltQueueSQL(findLowestContestant);
        VoltTable validation[] = voltExecuteSQL();
        
        if(validation[0].getRowCount() < 1)
        {
        	return VoterDemoSStoreConstants.ERR_NOT_ENOUGH_CONTESTANTS;
        }
        
        int lowestContestant = (int)(validation[0].fetchRow(0).getLong(0));
        
        if(VoterDemoSStoreConstants.SOCKET_CONTROL)
        	VoterDemoSStoreUtil.waitForSignal();
        
        voltQueueSQL(deleteLowestContestant, lowestContestant);
        voltQueueSQL(deleteLowestVotes, lowestContestant);
        voltQueueSQL(deleteLeaderBoardStmt, lowestContestant);
        /**
        voltQueueSQL(deleteDemoTopBoard);
    	voltQueueSQL(deleteDemoTrendingBoard);
    	voltQueueSQL(deleteDemoVoteCount);
    	voltQueueSQL(deleteDemoWindowCount);
    	voltQueueSQL(updateDemoTopBoard);
    	voltQueueSQL(updateDemoTrendingBoard);
    	voltQueueSQL(updateDemoVoteCount);
    	voltQueueSQL(updateDemoWindowCount);
    	*/
        voltExecuteSQL(true);
        try {
			printResults();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
        // Set the return value to 0: successful vote
        return VoterDemoSStoreConstants.DELETE_SUCCESSFUL;
    }
}