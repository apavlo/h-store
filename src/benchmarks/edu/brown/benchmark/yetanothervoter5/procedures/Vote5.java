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

package edu.brown.benchmark.yetanothervoter5.procedures;

import org.voltdb.ProcInfo;
import org.voltdb.SQLStmt;
import org.voltdb.VoltProcedure;
import org.voltdb.VoltTable;
import org.voltdb.types.TimestampType;
import org.voltdb.StmtInfo;


import edu.brown.benchmark.yetanothervoter5.VoterConstants;

@ProcInfo (
    singlePartition = true
)
public class Vote5 extends VoltProcedure {
	
    protected void toSetTriggerTableName()
    {   
        // set which stream will be used to trigger this frontend procedure
		addTriggerTable("S4");
    }

    
    @StmtInfo(
            upsertable=true
        )
    public final SQLStmt insertVotesByContestantNumberStateStmt = 
    new SQLStmt("INSERT INTO votes_by_contestant_number_state ( contestant_number, state, num_votes ) SELECT S4.contestant_number, S4.state, S4.num_votes+1 FROM S4;");

    public final SQLStmt removeS4 = new SQLStmt(
            "DELETE FROM S4;"
        );


    public long run() {
            
        voltQueueSQL(insertVotesByContestantNumberStateStmt);
        voltExecuteSQL();

		voltQueueSQL(removeS4);
		voltExecuteSQL();

        // Set the return value to 0: successful vote
        return VoterConstants.VOTE_SUCCESSFUL;
    }
}
