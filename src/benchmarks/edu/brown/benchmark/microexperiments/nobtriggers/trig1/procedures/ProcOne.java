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

package edu.brown.benchmark.microexperiments.nobtriggers.trig1.procedures;

import org.voltdb.ProcInfo;
import org.voltdb.SQLStmt;
import org.voltdb.VoltProcedure;
import org.voltdb.VoltTable;
import org.voltdb.types.TimestampType;

import edu.brown.benchmark.microexperiments.nobtriggers.trig1.NoBTriggersConstants;

@ProcInfo (
    //partitionInfo = "a_tbl.a_id:1",
    singlePartition = true
)
public class ProcOne extends VoltProcedure {
	
    // Checks if the vote is for a valid contestant
    public final SQLStmt insertStmt = new SQLStmt(
	   "INSERT INTO a_str1 VALUES (?,?);"
    );
    
    public final SQLStmt insertS1Stmt = 
        new SQLStmt("INSERT INTO a_str2 SELECT * FROM a_str1 WHERE a_val > 1;");

    public final SQLStmt insertATbl1Stmt = 
        new SQLStmt("INSERT INTO a_tbl SELECT * FROM a_str1 WHERE a_val = 1;");
    
    public final SQLStmt deleteS1Stmt = new SQLStmt("DELETE FROM a_str1;");
    
    public final SQLStmt insertS2Stmt = 
           new SQLStmt("INSERT INTO a_str3 SELECT * FROM a_str2 WHERE a_val > 2;");
        
    public final SQLStmt insertATbl2Stmt = 
           new SQLStmt("INSERT INTO a_tbl SELECT * FROM a_str2 WHERE a_val = 2;");

    public final SQLStmt deleteS2Stmt = new SQLStmt("DELETE FROM a_str2;");
    
    public final SQLStmt insertS3Stmt = 
        new SQLStmt("INSERT INTO a_str4 SELECT * FROM a_str3 WHERE a_val > 3;");
    
    public final SQLStmt insertATbl3Stmt = 
            new SQLStmt("INSERT INTO a_tbl SELECT * FROM a_str3 WHERE a_val = 3;");
    
    public final SQLStmt deleteS3Stmt = new SQLStmt("DELETE FROM a_str3;");

    public final SQLStmt insertS4Stmt = 
        new SQLStmt("INSERT INTO a_str5 SELECT * FROM a_str4 WHERE a_val > 4;");

    public final SQLStmt insertATbl4Stmt = 
            new SQLStmt("INSERT INTO a_tbl SELECT * FROM a_str4 WHERE a_val = 4;");
    
    public final SQLStmt deleteS4Stmt = new SQLStmt("DELETE FROM a_str4;");

    public final SQLStmt insertS5Stmt = 
        new SQLStmt("INSERT INTO a_str6 SELECT * FROM a_str5 WHERE a_val > 5;");
    
    public final SQLStmt insertATbl5Stmt = 
            new SQLStmt("INSERT INTO a_tbl SELECT * FROM a_str5 WHERE a_val = 5;");
    
    public final SQLStmt deleteS5Stmt = new SQLStmt("DELETE FROM a_str5;");
    
    public final SQLStmt insertS6Stmt = 
        new SQLStmt("INSERT INTO a_str7 SELECT * FROM a_str6 WHERE a_val > 6;");
    
    public final SQLStmt insertATbl6Stmt = 
            new SQLStmt("INSERT INTO a_tbl SELECT * FROM a_str6 WHERE a_val = 6;");
    
    public final SQLStmt deleteS6Stmt = new SQLStmt("DELETE FROM a_str6;");

    public final SQLStmt insertS7Stmt = 
        new SQLStmt("INSERT INTO a_str8 SELECT * FROM a_str7 WHERE a_val > 7;");
    
    public final SQLStmt insertATbl7Stmt = 
            new SQLStmt("INSERT INTO a_tbl SELECT * FROM a_str7 WHERE a_val = 7;");
    
    public final SQLStmt deleteS7Stmt = new SQLStmt("DELETE FROM a_str7;");

    public final SQLStmt insertS8Stmt = 
        new SQLStmt("INSERT INTO a_str9 SELECT * FROM a_str8 WHERE a_val > 8;");
    
    public final SQLStmt insertATbl8Stmt = 
            new SQLStmt("INSERT INTO a_tbl SELECT * FROM a_str8 WHERE a_val = 8;");
    
    public final SQLStmt deleteS8Stmt = new SQLStmt("DELETE FROM a_str8;");

    public final SQLStmt insertS9Stmt = 
        new SQLStmt("INSERT INTO a_str10 SELECT * FROM a_str9 WHERE a_val > 9;");
    
    public final SQLStmt insertATbl9Stmt = 
        new SQLStmt("INSERT INTO a_tbl SELECT * FROM a_str9 WHERE a_val = 9;");
    
    public final SQLStmt deleteS9Stmt = new SQLStmt("DELETE FROM a_str9;");

    public final SQLStmt insertATbl10Stmt = 
        new SQLStmt("INSERT INTO a_tbl SELECT * FROM a_str10 WHERE a_val = 10;");
    
    public final SQLStmt deleteS10Stmt = new SQLStmt("DELETE FROM a_str10;");


    public long run(int row_id, int row_val) {
		
        // Queue up validation statements
        voltQueueSQL(insertStmt, row_id, row_val);
        voltExecuteSQL(true);
        if(NoBTriggersConstants.NUM_TRIGGERS >= 1)
        {
        	voltQueueSQL(insertS1Stmt);
        	voltQueueSQL(insertATbl1Stmt);
        	voltQueueSQL(deleteS1Stmt);
        	voltExecuteSQL(true);
        }
        else
        {
        	return NoBTriggersConstants.PROC_ONE_SUCCESSFUL;
        }
        
        if(NoBTriggersConstants.NUM_TRIGGERS >= 2)
        {
        	voltQueueSQL(insertS2Stmt);
        	voltQueueSQL(insertATbl2Stmt);
        	voltQueueSQL(deleteS2Stmt);
        	voltExecuteSQL(true);
        }
        else
        {
        	return NoBTriggersConstants.PROC_ONE_SUCCESSFUL;
        }
        
        if(NoBTriggersConstants.NUM_TRIGGERS >= 3)
        {
        	voltQueueSQL(insertS3Stmt);
        	voltQueueSQL(insertATbl3Stmt);
        	voltQueueSQL(deleteS3Stmt);
        	voltExecuteSQL(true);
        }
        else
        {
        	return NoBTriggersConstants.PROC_ONE_SUCCESSFUL;
        }
        
        if(NoBTriggersConstants.NUM_TRIGGERS >= 4)
        {
        	voltQueueSQL(insertS4Stmt);
        	voltQueueSQL(insertATbl4Stmt);
        	voltQueueSQL(deleteS4Stmt);
        	voltExecuteSQL(true);
        }
        else
        {
        	return NoBTriggersConstants.PROC_ONE_SUCCESSFUL;
        }
        
        if(NoBTriggersConstants.NUM_TRIGGERS >= 5)
        {
        	voltQueueSQL(insertS5Stmt);
        	voltQueueSQL(insertATbl5Stmt);
        	voltQueueSQL(deleteS5Stmt);
        	voltExecuteSQL(true);
        }
        else
        {
        	return NoBTriggersConstants.PROC_ONE_SUCCESSFUL;
        }
        
        if(NoBTriggersConstants.NUM_TRIGGERS >= 6)
        {
        	voltQueueSQL(insertS6Stmt);
        	voltQueueSQL(insertATbl6Stmt);
        	voltQueueSQL(deleteS6Stmt);
        	voltExecuteSQL(true);
        }
        else
        {
        	return NoBTriggersConstants.PROC_ONE_SUCCESSFUL;
        }
        
        if(NoBTriggersConstants.NUM_TRIGGERS >= 7)
        {
        	voltQueueSQL(insertS7Stmt);
        	voltQueueSQL(insertATbl7Stmt);
        	voltQueueSQL(deleteS7Stmt);
        	voltExecuteSQL(true);
        }
        else
        {
        	return NoBTriggersConstants.PROC_ONE_SUCCESSFUL;
        }
        
        if(NoBTriggersConstants.NUM_TRIGGERS >= 8)
        {
        	voltQueueSQL(insertS8Stmt);
        	voltQueueSQL(insertATbl8Stmt);
        	voltQueueSQL(deleteS8Stmt);
        	voltExecuteSQL(true);
        }
        else
        {
        	return NoBTriggersConstants.PROC_ONE_SUCCESSFUL;
        }
        
        if(NoBTriggersConstants.NUM_TRIGGERS >= 9)
        {
        	voltQueueSQL(insertS9Stmt);
        	voltQueueSQL(insertATbl9Stmt);
        	voltQueueSQL(deleteS9Stmt);
        	voltExecuteSQL(true);
        }
        else
        {
        	return NoBTriggersConstants.PROC_ONE_SUCCESSFUL;
        }
        
        if(NoBTriggersConstants.NUM_TRIGGERS >= 10)
        {
        	voltQueueSQL(insertATbl10Stmt);
        	voltQueueSQL(deleteS10Stmt);
        	voltExecuteSQL(true);
        }
				
        // Set the return value to 0: successful vote
        return NoBTriggersConstants.PROC_ONE_SUCCESSFUL;
    }
}