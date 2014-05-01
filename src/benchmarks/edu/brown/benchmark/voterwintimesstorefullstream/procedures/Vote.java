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

package edu.brown.benchmark.voterwintimesstorefullstream.procedures;

import org.voltdb.ProcInfo;
import org.voltdb.SQLStmt;
import org.voltdb.StmtInfo;
import org.voltdb.VoltProcedure;
import org.voltdb.VoltTable;
//import org.voltdb.types.TimestampType;

import edu.brown.benchmark.voterwintimesstorefullstream.VoterWinTimeSStoreFullStreamConstants;

@ProcInfo (
    partitionInfo = "votes.phone_number:1",
    singlePartition = true
)
public class Vote extends VoltProcedure {
	
	public final SQLStmt stagedVoteStmt = 
	    	new SQLStmt("INSERT INTO staged_votes_by_phone_number (phone_number, num_votes) VALUES (?, 0);");
	/**
	@StmtInfo(
            upsertable=true
        )
	public final SQLStmt updateStagedVoteStmt = 
	    	new SQLStmt("INSERT INTO staged_votes_by_phone_number (phone_number, num_votes) SELECT v_votes_by_phone_number.phone_number, v_votes_by_phone_number.num_votes FROM v_votes_by_phone_number, staged_votes_by_phone_number WHERE v_votes_by_phone_number.phone_number=staged_votes_by_phone_number.phone_number;");
	
	public final SQLStmt deleteStagedVoteStmt = 
	    	new SQLStmt("DELETE FROM staged_votes_by_phone_number;");
	*/
	
    // Records a vote
    public final SQLStmt insertVoteStmt = new SQLStmt(
		"INSERT INTO votes_stream (vote_id, phone_number, area_code, contestant_number, time) VALUES (?, ?, ?, ?, ?);"
    );
    
    public final SQLStmt insertS1Stmt = new SQLStmt(
		"INSERT INTO S1 (vote_id, phone_number, area_code, contestant_number, time) VALUES (?, ?, ?, ?, ?);"
    );
    
    public final SQLStmt insertS2Stmt = new SQLStmt(
		"INSERT INTO S2 (vote_id, phone_number, state, contestant_number, time) VALUES (?, ?, ?, ?, ?);"
    );
    
    public final SQLStmt insertS3Stmt = new SQLStmt(
		"INSERT INTO S3 (vote_id, phone_number, state, contestant_number, time) VALUES (?, ?, ?, ?, ?);"
    );
	
    public long run(long voteId, long phoneNumber, int contestantNumber, long maxVotesPerPhoneNumber, int currentTimestamp) {
			
        // Post the vote
        //TimestampType timestamp = new TimestampType();
    	//voltQueueSQL(stagedVoteStmt, phoneNumber);
    	//voltQueueSQL(updateStagedVoteStmt);
        //voltQueueSQL(insertVoteStmt, voteId, phoneNumber, (short)(phoneNumber / 10000000l), contestantNumber, currentTimestamp);
        //voltQueueSQL(deleteStagedVoteStmt);
    	
    	voltQueueSQL(insertS1Stmt, voteId, phoneNumber, (short)(phoneNumber / 10000000l), contestantNumber, currentTimestamp);
    	//voltQueueSQL(insertS2Stmt, voteId, phoneNumber, "XX", contestantNumber, currentTimestamp);
        voltExecuteSQL(true);
		
        // Set the return value to 0: successful vote
        return VoterWinTimeSStoreFullStreamConstants.VOTE_SUCCESSFUL;
    }

}

