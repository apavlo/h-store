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

package edu.brown.benchmark.microexperiments.noroutetrig.trig1.procedures;

import org.voltdb.ProcInfo;
import org.voltdb.SQLStmt;
import org.voltdb.VoltProcedure;
import org.voltdb.VoltTable;
import org.voltdb.types.TimestampType;

import edu.brown.benchmark.microexperiments.noroutetrig.trig1.RouteTrigConstants;

@ProcInfo (
	//partitionInfo = "a_tbl.a_id:1",
    singlePartition = true
)
public class Proc9 extends VoltProcedure {
	
	public final SQLStmt getMinId = new SQLStmt(
		"SELECT min(a_id) FROM proc_nine_out;"
	);
	
	public final SQLStmt insertATableStmt = new SQLStmt(
		"INSERT INTO a_tbl VALUES (?, ?);"	
	);
	
	public final SQLStmt deleteProcStmt = new SQLStmt(
		"DELETE FROM proc_nine_out WHERE a_id = ?;"
	);
	
    public long run() {
		
    	voltQueueSQL(getMinId);
    	VoltTable[] v = voltExecuteSQL(true);
    	if(v[0].getRowCount() > 0)
    	{
    		long id = v[0].fetchRow(0).getLong(0);
    		voltQueueSQL(insertATableStmt, id, 9);
    		voltQueueSQL(deleteProcStmt, id);
    		voltExecuteSQL(true);
    	}
				
        // Set the return value to 0: successful vote
        return RouteTrigConstants.PROC_ONE_SUCCESSFUL;
    }
}