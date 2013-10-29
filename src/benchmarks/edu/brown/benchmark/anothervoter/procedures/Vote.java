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

package edu.brown.benchmark.anothervoter.procedures;

import org.voltdb.ProcInfo;
import org.voltdb.SQLStmt;
import org.voltdb.VoltProcedure;
import org.voltdb.VoltTable;
import org.voltdb.types.TimestampType;
import org.voltdb.StmtInfo;


import edu.brown.benchmark.anothervoter.VoterConstants;

@ProcInfo (
    partitionInfo = "votes.phone_number:1",
    singlePartition = true
)
public class Vote extends VoltProcedure {
	
    // Checks to determine if we need to initialize the voter information in table - votes_by_phone_number
    public final SQLStmt checkVoterStmt = new SQLStmt(
        "SELECT num_votes FROM votes_by_phone_number WHERE phone_number = ?;"
    );
    
    public final SQLStmt insertVoterStmt = new SQLStmt(
            "INSERT INTO votes_by_phone_number (phone_number, num_votes) VALUES (?, 0);"
        );

    // Checks if the vote is for a valid contestant
    public final SQLStmt checkContestantStmt = new SQLStmt(
	   "SELECT contestant_number FROM contestants WHERE contestant_number = ?;"
    );
	

    // Checks an area code to retrieve the corresponding state
    public final SQLStmt checkStateStmt = new SQLStmt(
		"SELECT state FROM area_code_state WHERE area_code = ?;"
    );
	
    // Records a vote
    public final SQLStmt insertVoteStmt = new SQLStmt(
		"INSERT INTO votes (vote_id, phone_number, state, contestant_number, created) VALUES (?, ?, ?, ?, ?);"
    );
    
    public final SQLStmt insertCurrentVoteStmt = new SQLStmt(
            "INSERT INTO CurrentVoteTable (vote_id, phone_number, state, contestant_number, created) VALUES (?, ?, ?, ?, ?);"
        );

    public final SQLStmt removeCurrentVoteStmt = new SQLStmt(
            "DELETE FROM CurrentVoteTable;"
        );

    @StmtInfo(
            upsertable=true
        )
    public final SQLStmt insertVotesByPhoneNumberStmt = 
    new SQLStmt("INSERT INTO votes_by_phone_number ( phone_number, num_votes ) SELECT votes_by_phone_number.phone_number, votes_by_phone_number.num_votes + 1 FROM votes_by_phone_number, CurrentVoteTable WHERE votes_by_phone_number.phone_number=CurrentVoteTable.phone_number;");

    @StmtInfo(
            upsertable=true
        )
    public final SQLStmt insertVotesByContestantNumberStateStmt = 
    new SQLStmt("INSERT INTO votes_by_contestant_number_state ( contestant_number, state, num_votes ) SELECT votes_by_contestant_number_state.contestant_number, votes_by_contestant_number_state.state, votes_by_contestant_number_state.num_votes+1 FROM votes_by_contestant_number_state, CurrentVoteTable WHERE (votes_by_contestant_number_state.contestant_number=CurrentVoteTable.contestant_number) and (votes_by_contestant_number_state.state=CurrentVoteTable.state);");

	
    public long run(long voteId, long phoneNumber, int contestantNumber, long maxVotesPerPhoneNumber) {
        // PHASE ONE : initialization
        // check if there is voter's record already
        voltQueueSQL(checkVoterStmt, phoneNumber);
        voltQueueSQL(checkStateStmt, (short)(phoneNumber / 10000000l));
        voltQueueSQL(checkContestantStmt, contestantNumber);
        VoltTable validation[] = voltExecuteSQL();
        
        if (validation[2].getRowCount() == 0) {
            return VoterConstants.ERR_INVALID_CONTESTANT;
        }

        // initialize the voter's record
        if (validation[0].getRowCount() == 0) {
            voltQueueSQL(insertVoterStmt, phoneNumber);
        }
        else
            if (validation[0].asScalarLong() >= maxVotesPerPhoneNumber) {
                return VoterConstants.ERR_VOTER_OVER_VOTE_LIMIT;
            }
        
        final String state = (validation[1].getRowCount() > 0) ? validation[1].fetchRow(0).getString(0) : "XX";
		 		
        // Post the vote
        TimestampType timestamp = new TimestampType();
        voltQueueSQL(insertVoteStmt, voteId, phoneNumber, state, contestantNumber, timestamp);
        voltQueueSQL(insertCurrentVoteStmt, voteId, phoneNumber, state, contestantNumber, timestamp);
        voltExecuteSQL();
        
        voltQueueSQL(insertVotesByPhoneNumberStmt);
        voltQueueSQL(insertVotesByContestantNumberStateStmt);
        voltExecuteSQL();

        voltQueueSQL(removeCurrentVoteStmt);
        voltExecuteSQL(true);

        // Set the return value to 0: successful vote
        return VoterConstants.VOTE_SUCCESSFUL;
    }
}
