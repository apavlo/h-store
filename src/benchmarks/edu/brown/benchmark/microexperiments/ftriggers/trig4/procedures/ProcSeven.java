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

package edu.brown.benchmark.microexperiments.ftriggers.trig4.procedures;

import org.voltdb.ProcInfo;
import org.voltdb.SQLStmt;
import org.voltdb.VoltProcedure;
import org.voltdb.VoltTable;
import org.voltdb.types.TimestampType;

import edu.brown.benchmark.microexperiments.ftriggers.trig4.FTriggersConstants;

@ProcInfo (
	//partitionInfo = "a_tbl.a_id:1",
    singlePartition = true
)
public class ProcSeven extends VoltProcedure {
	
	protected void toSetTriggerTableName()
	{
		addTriggerTable("proc_six_out");
	}
	
	public final SQLStmt insertATableStmt = new SQLStmt(
			"INSERT INTO a_tbl (a_id, a_val) SELECT * FROM proc_six_out;"
	);
	
    public final SQLStmt insertProcOutStmt = new SQLStmt(
    		"INSERT INTO proc_seven_out (a_id, a_val) SELECT * FROM proc_six_out;"
    );
    
    public final SQLStmt deleteProcOutStmt = new SQLStmt(
    		"DELETE FROM proc_six_out;"
    );
	
    public long run() {
		
    	if(FTriggersConstants.NUM_TRIGGERS > 7)
    		voltQueueSQL(insertProcOutStmt);
    	else
    		voltQueueSQL(insertATableStmt);
    	
    	voltQueueSQL(deleteProcOutStmt);
        voltExecuteSQL(true);
				
        // Set the return value to 0: successful vote
        return FTriggersConstants.PROC_THREE_SUCCESSFUL;
    }
}