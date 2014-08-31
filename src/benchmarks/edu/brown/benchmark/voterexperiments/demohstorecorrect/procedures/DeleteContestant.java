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

package edu.brown.benchmark.voterexperiments.demohstorecorrect.procedures;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.ArrayList;

import org.voltdb.ProcInfo;
import org.voltdb.SQLStmt;
import org.voltdb.StmtInfo;
import org.voltdb.VoltProcedure;
import org.voltdb.VoltTable;
import org.voltdb.types.TimestampType;

import edu.brown.benchmark.voterexperiments.demohstorecorrect.VoterDemoHStoreConstants;
import edu.brown.benchmark.voterexperiments.demohstorecorrect.VoterDemoHStoreUtil;

@ProcInfo (
	//partitionInfo = "contestants.contestant_number:1",
    singlePartition = true
)
public class DeleteContestant extends VoltProcedure {
    
	public final SQLStmt findLowestContestant = new SQLStmt(
			"SELECT vc.contestant_number, vc.num_votes, c.contestant_name FROM v_votes_by_contestant vc " +
			"JOIN contestants c ON c.contestant_number = vc.contestant_number " +
			"ORDER BY num_votes ASC, vc.contestant_number DESC LIMIT 1;"
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
    
    public final SQLStmt insertRemovedContestant = new SQLStmt(
    	"INSERT INTO removed_contestant VALUES (?,?,?);"	
    );
    
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
							  + "        , contestant_number DESC"
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
			  + " ORDER BY num_votes DESC"
			  + "        , contestant_number ASC");
	
	public final SQLStmt getVoteCountStmt = new SQLStmt( "SELECT totalcnt FROM proc_one_count WHERE row_id=1;");
	public final SQLStmt getSuccessCountStmt = new SQLStmt( "SELECT successcnt FROM proc_one_count WHERE row_id=1;");
	public final SQLStmt getActualVoteCountStmt = new SQLStmt( "SELECT totalcnt, successcnt FROM proc_one_count WHERE row_id = 1;");
	public final SQLStmt getTrendingCountStmt = new SQLStmt("SELECT count(*) FROM w_rows;");
	public final SQLStmt getRemainingContestants = new SQLStmt("SELECT count(*) FROM contestants;");
	public final SQLStmt getRemovedContestants = new SQLStmt("SELECT row_id, contestant_name, num_votes FROM removed_contestant ORDER BY row_id DESC;");
	public final SQLStmt getAllRemainingContestants = new SQLStmt("SELECT c.contestant_name, v.num_votes FROM v_votes_by_contestant v JOIN contestants c ON v.contestant_number = c.contestant_number ORDER BY num_votes DESC;");
	public final SQLStmt getVotesTilNextDeleteStmt = new SQLStmt( "SELECT cnt FROM votes_next_delete WHERE row_id=1;");
	/////////////////////////////
	//END GET RESULTS
	/////////////////////////////
	
	public final SQLStmt updateVotesTilNextDeleteStmt = new SQLStmt(
		"UPDATE votes_next_delete SET cnt = ? WHERE row_id = 1;"
    );
	
	private void printResults() throws IOException
	{
		ArrayList<String> tableNames = new ArrayList<String>();
		if(!VoterDemoHStoreConstants.DEBUG)
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
			voltQueueSQL(getRemainingContestants);
			tableNames.add("remainingContestants");
	        voltQueueSQL(getVotesTilNextDeleteStmt);
	        tableNames.add("VotesTilNextDelete");
	        voltQueueSQL(getSuccessCountStmt);
	        tableNames.add("SuccessCount");
	        //voltQueueSQL(getRemovedContestant);
	        //tableNames.add("RemovedContestant");
		}
		else {
			voltQueueSQL(getAllVotesStmt);
			tableNames.add("Votes");
			voltQueueSQL(getActualVoteCountStmt);
			tableNames.add("ProcOneCounts");
		}
		
		VoltTable[] v = voltExecuteSQL();
		VoterDemoHStoreUtil.writeToFile(v, tableNames, VoterDemoHStoreConstants.DELETE_CODE);
		
		tableNames = new ArrayList<String>();
		voltQueueSQL(getAllRemainingContestants);
        tableNames.add("RemainingContestants");
        voltQueueSQL(getRemovedContestants);
        tableNames.add("RemovedContestants");
        v = voltExecuteSQL();
		VoterDemoHStoreUtil.writeToContestantsFile(v, tableNames, VoterDemoHStoreConstants.DELETE_CODE);
	}
    
    public long run() {
		
        voltQueueSQL(findLowestContestant);
        voltQueueSQL(getRemainingContestants);
        VoltTable validation[] = voltExecuteSQL();
        
        if(validation[1].getRowCount() < 1)
        {
        	return VoterDemoHStoreConstants.ERR_NOT_ENOUGH_CONTESTANTS;
        }
        
        int lowestContestant = (int)(validation[0].fetchRow(0).getLong(0));
        String lowestConName = validation[0].fetchRow(0).getString("contestant_name");
        int lowestConVotes = (int)(validation[0].fetchRow(0).getLong("num_votes"));
        long remainingContestants = validation[1].fetchRow(0).getLong(0);
        
        if(VoterDemoHStoreConstants.SOCKET_CONTROL)
        	VoterDemoHStoreUtil.waitForSignal();
        
        if(remainingContestants <= 1)
        {
        	return VoterDemoHStoreConstants.BM_FINISHED;
        }
        voltQueueSQL(insertRemovedContestant, remainingContestants, lowestConName, lowestConVotes);
        voltQueueSQL(deleteLowestContestant, lowestContestant);
        voltQueueSQL(deleteLowestVotes, lowestContestant);
        voltQueueSQL(deleteLeaderBoardStmt, lowestContestant);
        voltQueueSQL(updateVotesTilNextDeleteStmt, VoterDemoHStoreConstants.VOTE_THRESHOLD);

        voltExecuteSQL(true);
        try {
			printResults();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
        // Set the return value to 0: successful vote
        return VoterDemoHStoreConstants.DELETE_SUCCESSFUL;
    }
}