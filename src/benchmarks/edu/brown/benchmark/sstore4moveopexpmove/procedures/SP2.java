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

package edu.brown.benchmark.sstore4moveopexpmove.procedures;

import org.voltdb.ProcInfo;
import org.voltdb.SQLStmt;
import org.voltdb.StmtInfo;
import org.voltdb.VoltProcedure;
import org.voltdb.VoltTable;
import org.voltdb.types.TimestampType;

import edu.brown.benchmark.sstore4moveopexp.SStore4MoveOpExpConstants;

@ProcInfo (
	partitionInfo = "s1prime.part_id:0",
	partitionNum = 1,
	singlePartition = true
)
public class SP2 extends VoltProcedure {
	
	
	protected void toSetTriggerTableName()
	{
		addTriggerTable("s1prime");
	}
	
	public final SQLStmt pullFromS1Prime = new SQLStmt(
		"SELECT vote_id, part_id FROM s1prime WHERE part_id=1;"
	);
	
    public final SQLStmt ins2Stmt = new SQLStmt(
	   "INSERT INTO s2 (vote_id, part_id) VALUES (?, ?);"
    );
    
    public final SQLStmt clearS1Prime = new SQLStmt(
    	"DELETE FROM s1prime WHERE part_id=1;"
    );
    
    /**
     * Simulation of a computation
     */
    public final void compute() {
		try {
			Thread.sleep(1); // Sleep 1 millisecond
		} catch (InterruptedException ex) {
			Thread.currentThread().interrupt();
		}
    }
        	
    public long run(int part_id) {
		voltQueueSQL(pullFromS1Prime);
		VoltTable s1primeData[] = voltExecuteSQL();
		
//		compute();
		
		for (int i=0; i < s1primeData[0].getRowCount(); i++) {
			Long vote_id = s1primeData[0].fetchRow(i).getLong(0);
			voltQueueSQL(ins2Stmt, vote_id, part_id);
		}
		voltExecuteSQL();
		
        voltQueueSQL(clearS1Prime);
        voltExecuteSQL();
				
        // Set the return value to 0: successful vote
        return SStore4MoveOpExpConstants.VOTE_SUCCESSFUL;
    }
}
