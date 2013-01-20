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

import edu.brown.benchmark.ycsb.YCSBConstants; 

@ProcInfo(
    partitionInfo = "USERTABLE.YCSB_KEY: 0",
    singlePartition = true
)
public class InsertRecord extends VoltProcedure {
	
    public final SQLStmt insertStmt = new SQLStmt(
        "INSERT INTO USERTABLE VALUES (" +
            "?," +  // ID
            "?," +  // FIELD1
            "?," +  // FIELD2
            "?," +  // FIELD3
            "?," +  // FIELD4
            "?," +  // FIELD5
            "?," +  // FIELD6
            "?," +  // FIELD7
            "?," +  // FIELD8
            "?," +  // FIELD9
            "?)"    // FIELD10
    );

    public VoltTable[] run(long id, String fields[]) {
        assert(fields.length == YCSBConstants.NUM_COLUMNS);
		voltQueueSQL(insertStmt,
		        id,
		        fields[0], // FIELD1
                fields[1], // FIELD2
                fields[2], // FIELD3
                fields[3], // FIELD4
                fields[4], // FIELD5
                fields[5], // FIELD6
                fields[6], // FIELD7
                fields[7], // FIELD8
                fields[8], // FIELD9
                fields[9]  // FIELD10
        );
        return (voltExecuteSQL(true));
    }
}
