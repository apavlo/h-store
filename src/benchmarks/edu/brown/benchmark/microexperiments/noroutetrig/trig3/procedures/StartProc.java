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

package edu.brown.benchmark.microexperiments.noroutetrig.trig3.procedures;

import java.util.Random;

import org.voltdb.ProcInfo;
import org.voltdb.SQLStmt;
import org.voltdb.VoltProcedure;
import org.voltdb.VoltTable;
import org.voltdb.types.TimestampType;

import edu.brown.benchmark.microexperiments.noroutetrig.trig3.RouteTrigConstants;

@ProcInfo (
    //partitionInfo = "a_tbl.a_id:1",
    singlePartition = true
)
public class StartProc extends VoltProcedure {
	
	Random rand = new Random();
	
	public final SQLStmt insertProc0 = new SQLStmt(
		"INSERT INTO a_tbl VALUES (?,?);"
    );
	
    // Checks if the voter has exceeded their allowed number of votes
    public final SQLStmt insertProc1 = new SQLStmt(
		"INSERT INTO proc_one_out VALUES (?,?);"
    );
    
    public final SQLStmt insertProc2 = new SQLStmt(
		"INSERT INTO proc_two_out VALUES (?,?);"
    );
    
    public final SQLStmt insertProc3 = new SQLStmt(
		"INSERT INTO proc_three_out VALUES (?,?);"
    );
    
    public final SQLStmt insertProc4 = new SQLStmt(
		"INSERT INTO proc_four_out VALUES (?,?);"
    );
    
    public final SQLStmt insertProc5 = new SQLStmt(
		"INSERT INTO proc_five_out VALUES (?,?);"
    );
    
    public final SQLStmt insertProc6 = new SQLStmt(
		"INSERT INTO proc_six_out VALUES (?,?);"
    );
    
    public final SQLStmt insertProc7 = new SQLStmt(
		"INSERT INTO proc_seven_out VALUES (?,?);"
    );
    
    public final SQLStmt insertProc8 = new SQLStmt(
		"INSERT INTO proc_eight_out VALUES (?,?);"
    );
    
    public final SQLStmt insertProc9 = new SQLStmt(
		"INSERT INTO proc_nine_out VALUES (?,?);"
    );
    
    public final SQLStmt insertProc10 = new SQLStmt(
		"INSERT INTO proc_ten_out VALUES (?,?);"
    );
    
    public final SQLStmt[] insertProc = {
    		insertProc0,
    		insertProc1,
    		insertProc2,
    		insertProc3,
    		insertProc4,
    		insertProc5,
    		insertProc6,
    		insertProc7,
    		insertProc8,
    		insertProc9,
    		insertProc10,
	};

	
    public long run(int row_id, int row_val) {
		
        int next = rand.nextInt(RouteTrigConstants.NUM_TRIGGERS + 1);
    	
        if(next == 0)
        	voltQueueSQL(insertProc[0], row_id, 0);
        
        for(int i = 1; i <= RouteTrigConstants.NUM_TRIGGERS; i++)
        {
        	if(next == i)
        	{
        		voltQueueSQL(insertProc[i], row_id, row_val);
        	}
        }
        //voltQueueSQL(insertProc[0], row_id, 0);
        voltExecuteSQL(true);
				
        // Set the return value to 0: successful vote
        return (long)next;
    }
}