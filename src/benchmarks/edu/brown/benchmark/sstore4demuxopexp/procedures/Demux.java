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

import org.voltdb.ProcInfo;
import org.voltdb.SQLStmt;
import org.voltdb.StmtInfo;
import org.voltdb.VoltProcedure;
import org.voltdb.VoltTable;
import org.voltdb.types.TimestampType;
import java.util.HashMap;

import edu.brown.benchmark.sstore4demuxopexp.SStore4DemuxOpExpConstants;

@ProcInfo (
	partitionInfo = "s1.part_id:0",
	partitionNum = 0,
	singlePartition = true
)
public class Demux extends VoltProcedure {
    HashMap<Integer, SQLStmt> procMap = new HashMap<Integer, SQLStmt>();

    protected void toSetTriggerTableName() {
        addTriggerTable("s1");
    }

    public final SQLStmt pullFromS1 = new SQLStmt(
        "SELECT vote_id, part_id FROM s1;"
    );

    public final SQLStmt ins101Stmt = new SQLStmt(
	"INSERT INTO s101 (vote_id, part_id) VALUES (?, ?);"
    );

    public final SQLStmt ins102Stmt = new SQLStmt(
    	"INSERT INTO s102 (vote_id, part_id) VALUES (?, ?);"
    );
 
    public final SQLStmt ins103Stmt = new SQLStmt(
        "INSERT INTO s103 (vote_id, part_id) VALUES (?, ?);"
    );
 
    public final SQLStmt ins104Stmt = new SQLStmt(
        "INSERT INTO s104 (vote_id, part_id) VALUES (?, ?);"
    );

    public final SQLStmt ins105Stmt = new SQLStmt(
        "INSERT INTO s105 (vote_id, part_id) VALUES (?, ?);"
    );

    public final SQLStmt ins106Stmt = new SQLStmt(
        "INSERT INTO s106 (vote_id, part_id) VALUES (?, ?);"
    );

    public final SQLStmt ins107Stmt = new SQLStmt(
        "INSERT INTO s107 (vote_id, part_id) VALUES (?, ?);"
    );

    public final SQLStmt ins108Stmt = new SQLStmt(
        "INSERT INTO s108 (vote_id, part_id) VALUES (?, ?);"
    );

    public final SQLStmt ins109Stmt = new SQLStmt(
    	"INSERT INTO s109 (vote_id, part_id) VALUES (?, ?);"
    );

    public final SQLStmt ins110Stmt = new SQLStmt(
    	"INSERT INTO s110 (vote_id, part_id) VALUES (?, ?);"
    );

    public final SQLStmt ins111Stmt = new SQLStmt(
    	"INSERT INTO s111 (vote_id, part_id) VALUES (?, ?);"
   	);

    public final SQLStmt ins112Stmt = new SQLStmt(
    	"INSERT INTO s112 (vote_id, part_id) VALUES (?, ?);"
    );

    public final SQLStmt ins113Stmt = new SQLStmt(
    	"INSERT INTO s113 (vote_id, part_id) VALUES (?, ?);"
    );

    public final SQLStmt ins114Stmt = new SQLStmt(
    	"INSERT INTO s114 (vote_id, part_id) VALUES (?, ?);"
    );

    public final SQLStmt ins115Stmt = new SQLStmt(
    	"INSERT INTO s115 (vote_id, part_id) VALUES (?, ?);"
    );

    public final SQLStmt ins116Stmt = new SQLStmt(
    	"INSERT INTO s116 (vote_id, part_id) VALUES (?, ?);"
    );

    public final SQLStmt ins117Stmt = new SQLStmt(
    	"INSERT INTO s117 (vote_id, part_id) VALUES (?, ?);"
    );

    public final SQLStmt ins118Stmt = new SQLStmt(
    	"INSERT INTO s118 (vote_id, part_id) VALUES (?, ?);"
    );

    public final SQLStmt ins119Stmt = new SQLStmt(
    	"INSERT INTO s119 (vote_id, part_id) VALUES (?, ?);"
    );

    public final SQLStmt ins120Stmt = new SQLStmt(
    	"INSERT INTO s120 (vote_id, part_id) VALUES (?, ?);"
    );

    public final SQLStmt ins121Stmt = new SQLStmt(
    	"INSERT INTO s121 (vote_id, part_id) VALUES (?, ?);"
    );

    public final SQLStmt ins122Stmt = new SQLStmt(
    	"INSERT INTO s122 (vote_id, part_id) VALUES (?, ?);"
    );

    public final SQLStmt ins123Stmt = new SQLStmt(
    	"INSERT INTO s123 (vote_id, part_id) VALUES (?, ?);"
    );

    public final SQLStmt ins124Stmt = new SQLStmt(
    	"INSERT INTO s124 (vote_id, part_id) VALUES (?, ?);"
    );

    public final SQLStmt ins125Stmt = new SQLStmt(
    	"INSERT INTO s125 (vote_id, part_id) VALUES (?, ?);"
    );

    public final SQLStmt ins126Stmt = new SQLStmt(
    	"INSERT INTO s126 (vote_id, part_id) VALUES (?, ?);"
    );

    public final SQLStmt ins127Stmt = new SQLStmt(
    	"INSERT INTO s127 (vote_id, part_id) VALUES (?, ?);"
    );

    public final SQLStmt ins128Stmt = new SQLStmt(
    	"INSERT INTO s128 (vote_id, part_id) VALUES (?, ?);"
    );

    public final SQLStmt ins129Stmt = new SQLStmt(
    	"INSERT INTO s129 (vote_id, part_id) VALUES (?, ?);"
    );

    public final SQLStmt ins130Stmt = new SQLStmt(
    	"INSERT INTO s130 (vote_id, part_id) VALUES (?, ?);"
    );

    public final SQLStmt ins131Stmt = new SQLStmt(
    	"INSERT INTO s131 (vote_id, part_id) VALUES (?, ?);"
    );

    public final SQLStmt clearS1 = new SQLStmt(
    	"DELETE FROM s1;"
    );

    public Demux() {
        procMap.put(0, ins101Stmt);
        procMap.put(1, ins102Stmt);
        procMap.put(2, ins103Stmt);
        procMap.put(3, ins104Stmt);
        procMap.put(4, ins105Stmt);
        procMap.put(5, ins106Stmt);
        procMap.put(6, ins107Stmt);
        procMap.put(7, ins108Stmt);
        procMap.put(8, ins109Stmt);
        procMap.put(9, ins110Stmt);
        procMap.put(10, ins111Stmt);
        procMap.put(11, ins112Stmt);
        procMap.put(12, ins113Stmt);
        procMap.put(13, ins114Stmt);
        procMap.put(14, ins115Stmt);
        procMap.put(15, ins116Stmt);
        procMap.put(16, ins117Stmt);
        procMap.put(17, ins118Stmt);
        procMap.put(18, ins119Stmt);
        procMap.put(19, ins120Stmt);
        procMap.put(20, ins121Stmt);
        procMap.put(21, ins122Stmt);
        procMap.put(22, ins123Stmt);
        procMap.put(23, ins124Stmt);
        procMap.put(24, ins125Stmt);
        procMap.put(25, ins126Stmt);
        procMap.put(26, ins127Stmt);
        procMap.put(27, ins128Stmt);
        procMap.put(28, ins129Stmt);
        procMap.put(29, ins130Stmt);
        procMap.put(30, ins131Stmt);
    }

    public long run(int part_id) {
	voltQueueSQL(pullFromS1);
	VoltTable s1Data[] = voltExecuteSQLForceSinglePartition();

	for (int i=0; i < s1Data[0].getRowCount(); i++) {
	    int vote_id = (int)(s1Data[0].fetchRow(i).getLong(0));
	    voltQueueSQL(procMap.get(vote_id % 31), vote_id, part_id);
//	    voltQueueSQL(ins101Stmt, vote_id, part_id);
	}
	voltExecuteSQLForceSinglePartition();

        voltQueueSQL(clearS1);
        voltExecuteSQLForceSinglePartition();
				
        // Set the return value to 0: successful vote
        return SStore4DemuxOpExpConstants.VOTE_SUCCESSFUL;
    }
}
