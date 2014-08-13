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

package edu.brown.benchmark.sstore4demuxopexp.procedures;

import java.util.ArrayList;

import org.voltdb.ProcInfo;
import org.voltdb.SQLStmt;
import org.voltdb.StmtInfo;
import org.voltdb.VoltProcedure;
import org.voltdb.VoltTable;
import org.voltdb.types.TimestampType;

import edu.brown.benchmark.sstore4demuxopexp.SStore4DemuxOpExpConstants;

@ProcInfo (
	singlePartition = true
)
public class Throughput extends VoltProcedure {
	
	
	public final SQLStmt pullFromS21 = new SQLStmt(
		"SELECT count(*) FROM S21prime;"
	);
	
	public final SQLStmt pullFromS22 = new SQLStmt(
		"SELECT count(*) FROM S22prime;"
	);
		
	public final SQLStmt pullFromS23 = new SQLStmt(
		"SELECT count(*) FROM S23prime;"
	);
	
	public final SQLStmt updateTxnTotal = new SQLStmt(
		"INSERT INTO txntotal VALUES (?);"
	);
	
	public final SQLStmt getTxnTotal = new SQLStmt(
		"SELECT * FROM txntotal;"
	);
		
    public VoltTable[] run() {
    	Long tupleCount = (long) 0;
    	ArrayList<SQLStmt> pullList = new ArrayList<SQLStmt>();
    	pullList.add(pullFromS21);
    	pullList.add(pullFromS22);
    	pullList.add(pullFromS23);
    	for (SQLStmt s : pullList) {
    		voltQueueSQL(s);
    		VoltTable sData[] = voltExecuteSQL();
    		for (int i=0; i < sData[0].getRowCount(); i++) {
    			tupleCount += sData[0].fetchRow(i).getLong(0);
    		}
    	}
		
    	voltQueueSQL(updateTxnTotal, tupleCount);
    	voltExecuteSQL();
    	
    	voltQueueSQL(getTxnTotal);
    	return voltExecuteSQL();
    }
}