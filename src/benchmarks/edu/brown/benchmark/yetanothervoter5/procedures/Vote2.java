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
public class Vote2 extends VoltProcedure {
	
    protected void toSetTriggerTableName()
    {   
        // set which stream will be used to trigger this frontend procedure
		addTriggerTable("S1");
    }

    
    public final SQLStmt checkS1 = new SQLStmt(
            "SELECT * FROM S1;"
        );

    public final SQLStmt removeS1 = new SQLStmt(
            "DELETE FROM S1;"
        );


    public final SQLStmt insertS2 = new SQLStmt(
            "INSERT INTO S2 (vote_id, phone_number, state, contestant_number, created) SELECT S1.* FROM S1, votes_by_phone_number WHERE (S1.phone_number=votes_by_phone_number.phone_number) and (votes_by_phone_number.num_votes<10);"
        );
	
    public long run() {
        
        voltQueueSQL(checkS1);
        VoltTable s1_result[] = voltExecuteSQL();
        if (s1_result[0].getRowCount() == 1) {
            voltQueueSQL(insertS2);
            voltExecuteSQL();
        }

        voltQueueSQL(removeS1);
        voltExecuteSQL(true);

        // Set the return value to 0: successful vote
        return VoterConstants.VOTE_SUCCESSFUL;
    }
}
