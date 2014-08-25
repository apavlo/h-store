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

import java.io.IOException;
import java.util.ArrayList;

import org.voltdb.ProcInfo;
import org.voltdb.SQLStmt;
import org.voltdb.StmtInfo;
import org.voltdb.VoltProcedure;
import org.voltdb.VoltTable;
import org.voltdb.types.TimestampType;

import edu.brown.benchmark.voterexperiments.demosstorecorrect.VoterDemoSStoreUtil;
import edu.brown.benchmark.voterexperiments.demosstorecorrect.VoterDemoSStoreConstants;

@ProcInfo (
    partitionInfo = "votes.phone_number:1",
    singlePartition = true
)
public class Vote extends VoltProcedure {
	
	@StmtInfo(
            upsertable=true
        )
	
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
		"INSERT INTO votes (vote_id, phone_number, state, contestant_number, created) VALUES (?, ?, ?, ?, ?);"
    );
    
    // Records a vote
    public final SQLStmt insertProcEndStmt = new SQLStmt(
		"INSERT INTO proc_one_out (vote_id, phone_number, state, contestant_number, created) VALUES (?, ?, ?, ?, ?);"
    );
    
    public final SQLStmt getNumProcOneStmt = new SQLStmt(
		"SELECT totalcnt, successcnt FROM proc_one_count WHERE row_id = 1;"
    );
    
    public final SQLStmt updateNumProcOneStmt = new SQLStmt(
		"UPDATE proc_one_count SET totalcnt = ? WHERE row_id = 1;"
    );
    
    public final SQLStmt updateNumSuccessStmt = new SQLStmt(
		"UPDATE proc_one_count SET successcnt = ? WHERE row_id = 1;"
    );
    
    public final SQLStmt getNumContestants = new SQLStmt(
		"SELECT count(*) from contestants;"
    );
    
    public final SQLStmt updateVotesTilNextDeleteStmt = new SQLStmt(
		"UPDATE votes_next_delete SET cnt = ? WHERE row_id = 1;"
    );
    
	/////////////////////////////
	//BEGIN GET RESULTS
	/////////////////////////////
	// Gets the results
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
	public final SQLStmt getActualVoteCountStmt = new SQLStmt( "SELECT totalcnt, successcnt FROM proc_one_count WHERE row_id = 1;");
	public final SQLStmt getTrendingCountStmt = new SQLStmt("SELECT count(*) FROM trending_leaderboard;");
	public final SQLStmt getRemainingContestants = new SQLStmt("SELECT count(*) FROM contestants;");
	public final SQLStmt getRemovedContestant = new SQLStmt("SELECT contestant_name, num_votes FROM removed_contestant WHERE row_id = 1;");
	public final SQLStmt getVotesTilNextDeleteStmt = new SQLStmt( "SELECT cnt FROM votes_next_delete WHERE row_id=1;");
	/////////////////////////////
	//END GET RESULTS
	/////////////////////////////
    
    
    private void printResults(int numVotes) throws IOException
    {
    	//System.out.println(stat_filename + " : " + content );
        
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
            voltQueueSQL(getRemainingContestants);
	        tableNames.add("RemainingContestants");
	        voltQueueSQL(getVotesTilNextDeleteStmt);
	        tableNames.add("VotesTilNextDelete");
	        //voltQueueSQL(getRemovedContestant);
	        //tableNames.add("RemovedContestant");
        }
        else
        {
	        voltQueueSQL(getAllVotesStmt);
	    	tableNames.add("Votes");
	        voltQueueSQL(getActualVoteCountStmt);
			tableNames.add("ProcOneCounts");
        }
        
        VoltTable[] v = voltExecuteSQL();
        VoterDemoSStoreUtil.writeToFile(v, tableNames, numVotes);
        
    }
    
	
    public long run(long voteId, long phoneNumber, int contestantNumber, long maxVotesPerPhoneNumber) {
		
		long procOutput = VoterDemoSStoreConstants.STATUS_NOT_DETERMINED;
		voltQueueSQL(getNumProcOneStmt);
		voltQueueSQL(getRemainingContestants);
		VoltTable validation[] = voltExecuteSQL();
		
		long numProcOne = validation[0].fetchRow(0).getLong(0) + 1;
		long numSuccess = validation[0].fetchRow(0).getLong(1);
		long remainingContestants = validation[1].fetchRow(0).getLong(0);
		
        // Queue up validation statements
		voltQueueSQL(checkContestantStmt, contestantNumber);
        voltQueueSQL(checkVoterStmt, phoneNumber);
        voltQueueSQL(checkStateStmt, (short)(phoneNumber / 10000000l));
        voltQueueSQL(updateNumProcOneStmt, numProcOne);
        //voltQueueSQL(getNumContestants);
        validation = voltExecuteSQL();
		
        // validate the maximum limit for votes number
        if (validation[0].getRowCount() == 0) {
        	procOutput = VoterDemoSStoreConstants.ERR_INVALID_CONTESTANT;
        }
        
        if ((validation[1].getRowCount() == 1) &&
			(validation[1].fetchRow(0).getLong(0) >= maxVotesPerPhoneNumber)) {
        	procOutput = VoterDemoSStoreConstants.ERR_VOTER_OVER_VOTE_LIMIT;
        }
		
        // Some sample client libraries use the legacy random phone generation that mostly
        // created invalid phone numbers. Until refactoring, re-assign all such votes to
        // the "XX" fake state (those votes will not appear on the Live Statistics dashboard,
        // but are tracked as legitimate instead of invalid, as old clients would mostly get
        // it wrong and see all their transactions rejected).
        final String state = (validation[2].getRowCount() > 0) ? validation[2].fetchRow(0).getString(0) : "XX";
        
		 	
        if(procOutput == VoterDemoSStoreConstants.STATUS_NOT_DETERMINED)
        {
        	// Post the vote
		    TimestampType timestamp = new TimestampType();
		    voltQueueSQL(insertVoteStmt, voteId, phoneNumber, state, contestantNumber, timestamp);
		    voltQueueSQL(insertProcEndStmt, voteId, phoneNumber, state, contestantNumber, timestamp);
		    voltQueueSQL(updateNumSuccessStmt, numSuccess + 1);
	        int votesSinceLastDelete = (((int)numSuccess) % VoterDemoSStoreConstants.VOTE_THRESHOLD) + 1;
	        voltQueueSQL(updateVotesTilNextDeleteStmt, VoterDemoSStoreConstants.VOTE_THRESHOLD - votesSinceLastDelete);
		    voltExecuteSQL(true);
		    procOutput = VoterDemoSStoreConstants.VOTE_SUCCESSFUL;
        }
        
        // Set the return value to 0: successful vote
        if(((int)numProcOne % (int)VoterDemoSStoreConstants.BOARD_REFRESH) == 0)
        {
        	if(VoterDemoSStoreConstants.SOCKET_CONTROL)
        		VoterDemoSStoreUtil.waitForSignal();
        	try {
        		if(remainingContestants > 1)
        			printResults((int)numProcOne);
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
        }
		
        // Set the return value to 0: successful vote
        return procOutput;
    }
}