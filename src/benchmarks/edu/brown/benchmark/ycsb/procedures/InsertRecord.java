/***************************************************************************
 *  Copyright (C) 2012 by H-Store Project                                  *
 *  Brown University                                                       *
 *  Massachusetts Institute of Technology                                  *
 *  Yale University                                                        *
 *                                                                         *
 *  Coded By:  Justin A. DeBrabant (http://www.cs.brown.edu/~debrabant/)   *								   
 *                                                                         *
 *                                                                         *
 *  Permission is hereby granted, free of charge, to any person obtaining  *
 *  a copy of this software and associated documentation files (the        *
 *  "Software"), to deal in the Software without restriction, including    *
 *  without limitation the rights to use, copy, modify, merge, publish,    *
 *  distribute, sublicense, and/or sell copies of the Software, and to     *
 *  permit persons to whom the Software is furnished to do so, subject to  *
 *  the following conditions:                                              *
 *                                                                         *
 *  The above copyright notice and this permission notice shall be         *
 *  included in all copies or substantial portions of the Software.        *
 *                                                                         *
 *  THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,        *
 *  EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF     *
 *  MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. *
 *  IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR      *
 *  OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,  *
 *  ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR  *
 *  OTHER DEALINGS IN THE SOFTWARE.                                        *
 ***************************************************************************/

package edu.brown.benchmark.ycsb.procedures;

import org.voltdb.ProcInfo;
import org.voltdb.SQLStmt;
import org.voltdb.VoltProcedure;
import org.voltdb.VoltTable;

import edu.brown.benchmark.ycsb.YCSBUtil; 
import edu.brown.benchmark.ycsb.YCSBConstants; 

@ProcInfo(
    partitionInfo = "USERTABLE.YCSB_KEY: 0",
    singlePartition = true
)
public class InsertRecord extends VoltProcedure {
	
    public final SQLStmt insertStmt = new SQLStmt("INSERT INTO USERTABLE VALUES (?,?,?,?,?,?,?,?,?,?,?)");

    public VoltTable[] run(long id) {
		
		voltQueueSQL(insertStmt, id,
		             YCSBUtil.astring(YCSBConstants.COLUMN_LENGTH, YCSBConstants.COLUMN_LENGTH), // FIELD1
		             YCSBUtil.astring(YCSBConstants.COLUMN_LENGTH, YCSBConstants.COLUMN_LENGTH), // FIELD2
		             YCSBUtil.astring(YCSBConstants.COLUMN_LENGTH, YCSBConstants.COLUMN_LENGTH), // FIELD3
		             YCSBUtil.astring(YCSBConstants.COLUMN_LENGTH, YCSBConstants.COLUMN_LENGTH), // FIELD4
		             YCSBUtil.astring(YCSBConstants.COLUMN_LENGTH, YCSBConstants.COLUMN_LENGTH), // FIELD5
		             YCSBUtil.astring(YCSBConstants.COLUMN_LENGTH, YCSBConstants.COLUMN_LENGTH), // FIELD6
		             YCSBUtil.astring(YCSBConstants.COLUMN_LENGTH, YCSBConstants.COLUMN_LENGTH), // FIELD7
		             YCSBUtil.astring(YCSBConstants.COLUMN_LENGTH, YCSBConstants.COLUMN_LENGTH), // FIELD8
		             YCSBUtil.astring(YCSBConstants.COLUMN_LENGTH, YCSBConstants.COLUMN_LENGTH), // FIELD9
		             YCSBUtil.astring(YCSBConstants.COLUMN_LENGTH, YCSBConstants.COLUMN_LENGTH)  // FIELD10
        );
        return (voltExecuteSQL(true));
    }
}
