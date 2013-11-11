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
public class Vote1 extends VoltProcedure {
	
    protected void toSetTriggerTableName()
    {   
        // set which stream will be used to trigger this frontend procedure
		addTriggerTable("votes_stream");
    }

	public final SQLStmt insertS1 = new SQLStmt(
         "INSERT INTO S1 (vote_id, phone_number, state, contestant_number, created) SELECT votes_stream.* FROM votes_stream, contestants WHERE votes_stream.contestant_number=contestants.contestant_number;"
    );

    public final SQLStmt removeVote = new SQLStmt(
            "DELETE FROM votes_stream;"
        );
    
	
    public long run() {
        
        voltQueueSQL(insertS1);
        voltExecuteSQL();

        voltQueueSQL(removeVote);
        voltExecuteSQL(true);

        // Set the return value to 0: successful vote
        return VoterConstants.VOTE_SUCCESSFUL;
    }
}
