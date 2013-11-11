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

package edu.brown.benchmark.yetanothervoter3.procedures;

import org.voltdb.ProcInfo;
import org.voltdb.SQLStmt;
import org.voltdb.VoltProcedure;
import org.voltdb.VoltTable;
import org.voltdb.types.TimestampType;
import org.voltdb.StmtInfo;


import edu.brown.benchmark.yetanothervoter3.VoterConstants;

@ProcInfo (
    singlePartition = true
)
public class Vote3 extends VoltProcedure {
	
    protected void toSetTriggerTableName()
    {   
        // set which stream will be used to trigger this frontend procedure
		addTriggerTable("S2");
    }

    
    public final SQLStmt checkS2 = new SQLStmt(
            "SELECT * FROM S2;"
        );

    public final SQLStmt removeS2 = new SQLStmt(
            "DELETE FROM S2;"
        );

    @StmtInfo(
            upsertable=true
        )
    public final SQLStmt insertVotesByPhoneNumberStmt = 
    new SQLStmt("INSERT INTO votes_by_phone_number ( phone_number, num_votes ) SELECT votes_by_phone_number.phone_number, votes_by_phone_number.num_votes + 1 FROM votes_by_phone_number, S2 WHERE votes_by_phone_number.phone_number=S2.phone_number;");

    @StmtInfo(
            upsertable=true
        )
    public final SQLStmt insertVotesByContestantNumberStateStmt = 
    new SQLStmt("INSERT INTO votes_by_contestant_number_state ( contestant_number, state, num_votes ) SELECT votes_by_contestant_number_state.contestant_number, votes_by_contestant_number_state.state, votes_by_contestant_number_state.num_votes+1 FROM votes_by_contestant_number_state, S2 WHERE (votes_by_contestant_number_state.contestant_number=S2.contestant_number) and (votes_by_contestant_number_state.state=S2.state);");

    public final SQLStmt insertVoteStmt = new SQLStmt(
            "INSERT INTO votes (vote_id, phone_number, state, contestant_number, created) SELECT * FROM S2;"
        );
        
	
    public long run() {
            
        voltQueueSQL(checkS2);
        VoltTable s2_result[] = voltExecuteSQL();
        if (s2_result[0].getRowCount() == 1) {
                voltQueueSQL(insertVoteStmt);
                voltQueueSQL(insertVotesByPhoneNumberStmt);
                voltQueueSQL(insertVotesByContestantNumberStateStmt);
                voltExecuteSQL();
        }
        voltQueueSQL(removeS2);
        voltExecuteSQL();

        // Set the return value to 0: successful vote
        return VoterConstants.VOTE_SUCCESSFUL;
    }
}
