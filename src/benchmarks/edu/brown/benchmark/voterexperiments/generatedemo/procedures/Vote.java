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
// contestant and that the voter (phone number of the caller) is not above the
// number of allowed votes.
//

package edu.brown.benchmark.voterexperiments.generatedemo.procedures;

import java.io.IOException;
import java.util.ArrayList;

import org.voltdb.ProcInfo;
import org.voltdb.SQLStmt;
import org.voltdb.VoltProcedure;
import org.voltdb.VoltTable;
import org.voltdb.types.TimestampType;

import edu.brown.benchmark.voterexperiments.generatedemo.VoterWinSStoreConstants;
import edu.brown.benchmark.voterexperiments.generatedemo.VoterWinSStoreUtil;

@ProcInfo (
    partitionInfo = "votes.phone_number:1",
    singlePartition = true
)
public class Vote extends VoltProcedure {
	
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
    
    public final SQLStmt checkNumVotesStmt = new SQLStmt(
		"SELECT votes_cnt, reset_cnt FROM votes_count WHERE row_id = 1;"
    );
    
    public final SQLStmt updateNumVotesStmt = new SQLStmt(
		"UPDATE votes_count SET votes_cnt = ? WHERE row_id = 1;"
    );
    
    public final SQLStmt updateResetVotesStmt = new SQLStmt(
		"UPDATE votes_count SET reset_cnt = ? WHERE row_id = 1;"
    );
    
//    public final SQLStmt selectLeaderboardStmt = new SQLStmt(
//		"SELECT * FROM LEADERBOARD ORDER BY CONTESTANT_NUMBER;"
//    );
    
    public final SQLStmt findLowestContestant = new SQLStmt(
		"SELECT * FROM v_votes_by_contestant ORDER BY num_votes ASC, contestant_number DESC LIMIT 1;"
    );
    
    public final SQLStmt findContestant = new SQLStmt(
		"SELECT contestant_number FROM contestants WHERE contestant_number = ?;"
    );
    
    public final SQLStmt getVotesToDelete = new SQLStmt(
		"SELECT * FROM votes WHERE contestant_number = ?;"
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
    
    public final SQLStmt updateLastDeletedStmt = new SQLStmt(
		"UPDATE last_deleted SET contestant_number = ? WHERE row_id = 1;"
    );
    
    public final SQLStmt checkContestantVotesStmt = new SQLStmt(
	   "SELECT contestant_number, num_votes FROM v_votes_by_contestant ORDER BY num_votes ASC, contestant_number DESC;"
    );
    
    public final SQLStmt findRemainingContestants = new SQLStmt(
	   "SELECT count(*) FROM contestants;"
    );
    
	/////////////////////////////
	//BEGIN GET RESULTS
	/////////////////////////////
	public final SQLStmt getAllVotesStmt = new SQLStmt( "   SELECT a.contestant_name   AS contestant_name"
			+ "        , b.contestant_number         AS contestant_number"
	+ "        , b.num_votes          AS num_votes"
	+ "     FROM v_votes_by_contestant b"
	+ "        , contestants AS a"
	+ "    WHERE a.contestant_number = b.contestant_number"
	+ " ORDER BY num_votes ASC"
	+ "        , contestant_number DESC");
	
	public final SQLStmt getVoteCountStmt = new SQLStmt( "SELECT votes_cnt, reset_cnt FROM votes_count WHERE row_id=1;");
	/////////////////////////////
	//END GET RESULTS
	/////////////////////////////
	
    public VoltTable[] run(long voteId, long phoneNumber, int contestantNumber, long maxVotesPerPhoneNumber) {
		
        // Queue up validation statements
    	voltQueueSQL(checkContestantStmt, contestantNumber);
        voltQueueSQL(checkVoterStmt, phoneNumber);
        voltQueueSQL(checkStateStmt, (short)(phoneNumber / 10000000l));
        voltQueueSQL(checkNumVotesStmt);
        VoltTable validation[] = voltExecuteSQL();
		
        if (validation[0].getRowCount() == 0) {
            return null;
        }
        
        // validate the maximum limit for votes number
        if ((validation[1].getRowCount() == 1) &&
			(validation[1].asScalarLong() >= maxVotesPerPhoneNumber)) {
            return null;
        }
		
        // Some sample client libraries use the legacy random phone generation that mostly
        // created invalid phone numbers. Until refactoring, re-assign all such votes to
        // the "XX" fake state (those votes will not appear on the Live Statistics dashboard,
        // but are tracked as legitimate instead of invalid, as old clients would mostly get
        // it wrong and see all their transactions rejected).
        final String state = (validation[2].getRowCount() > 0) ? validation[2].fetchRow(0).getString(0) : "XX";
		 		
        int votes_cnt = (int)validation[3].fetchRow(0).getLong("votes_cnt") + 1;
        int reset_cnt = (int)validation[3].fetchRow(0).getLong("reset_cnt") + 1;
        
        // Post the vote
        TimestampType timestamp = new TimestampType();
        voltQueueSQL(insertVoteStmt, voteId, phoneNumber, state, contestantNumber, timestamp);
        voltQueueSQL(updateNumVotesStmt, votes_cnt);
        voltQueueSQL(updateResetVotesStmt, (reset_cnt % VoterWinSStoreConstants.VOTE_THRESHOLD));
        //voltQueueSQL(selectLeaderboardStmt);
        voltExecuteSQL();
        
        if(reset_cnt % VoterWinSStoreConstants.PRINT_THRESHOLD == 0)
        {
        	ArrayList<String> tableNames = new ArrayList<String>();
            voltQueueSQL(getAllVotesStmt);
            tableNames.add("Votes");
    		voltQueueSQL(getVoteCountStmt);
    		tableNames.add("VoteCount");
    		VoltTable[] v = voltExecuteSQL();
    		try {
				VoterWinSStoreUtil.writeToFile(v, tableNames, votes_cnt);
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
        }
        
        voltQueueSQL(findLowestContestant);
        voltQueueSQL(findRemainingContestants);
        validation = voltExecuteSQL();
        
        int lowestContestant = (int)(validation[0].fetchRow(0).getLong(0));
        int remainingContestants = (int)(validation[1].fetchRow(0).getLong(0));
        // Set the return value to 0: successful vote
        if(reset_cnt == VoterWinSStoreConstants.VOTE_THRESHOLD && remainingContestants > 1)
        {
            
            ArrayList<String> tableNames = new ArrayList<String>();
            voltQueueSQL(getAllVotesStmt);
            tableNames.add("Votes");
    		voltQueueSQL(getVoteCountStmt);
    		tableNames.add("VoteCount");
    		VoltTable[] v = voltExecuteSQL();
    		try {
				VoterWinSStoreUtil.writeToFile(v, tableNames, VoterWinSStoreConstants.DELETE_CODE);
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
            
            voltQueueSQL(getVotesToDelete, lowestContestant);
            voltQueueSQL(findContestant, lowestContestant);
            voltQueueSQL(deleteLowestContestant, lowestContestant);
            voltQueueSQL(deleteLowestVotes, lowestContestant);
            voltQueueSQL(deleteLeaderBoardStmt, lowestContestant);
            voltQueueSQL(updateLastDeletedStmt, lowestContestant);
            voltQueueSQL(checkContestantVotesStmt);
        	voltQueueSQL(checkNumVotesStmt);
            //voltExecuteSQL();
        }
        else { 
        	voltQueueSQL(checkContestantVotesStmt);
        	voltQueueSQL(checkNumVotesStmt);
        }
        
        return voltExecuteSQL();
    }

}

