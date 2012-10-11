/***************************************************************************
 *  Copyright (C) 2009 by H-Store Project                                  *
 *  Brown University                                                       *
 *  Massachusetts Institute of Technology                                  *
 *  Yale University                                                        *
 *                                                                         *
 *  Original Version:                                                      *
 *  Zhe Zhang (zhe@cs.brown.edu)                                           *
 *                                                                         *
 *  Modifications by:                                                      *
 *  Andy Pavlo (pavlo@cs.brown.edu)                                        *
 *  http://www.cs.brown.edu/~pavlo/                                        *
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
package edu.brown.benchmark.tm1.procedures;

import java.util.Arrays;

import org.voltdb.ProcInfo;
import org.voltdb.SQLStmt;
import org.voltdb.VoltProcedure;
import org.voltdb.VoltTable;

import edu.brown.benchmark.tm1.TM1Constants;

@ProcInfo(
    partitionInfo = "SUBSCRIBER.S_ID: 0",
    singlePartition = true
)
public class UpdateSubscriberData extends VoltProcedure {

    public final SQLStmt update1 = new SQLStmt(
        "UPDATE " + TM1Constants.TABLENAME_SUBSCRIBER +
        "   SET bit_1 = ? WHERE s_id = ?"
    );

    public final SQLStmt update2 = new SQLStmt(
        "UPDATE " + TM1Constants.TABLENAME_SPECIAL_FACILITY + 
        "   SET data_a = ? WHERE s_id = ? AND sf_type = ?"
    );

    public long run(long s_id, long bit_1, long data_a, long sf_type) {
        voltQueueSQL(update1, bit_1, s_id);
        voltQueueSQL(update2, data_a, s_id, sf_type);
        VoltTable results[] = voltExecuteSQL(true);
        assert (results.length == 2) : "Expected 2 results but got " + results.length + "\n" + Arrays.toString(results);

        boolean adv = results[1].advanceRow();
        assert (adv);
        long rows_updated = results[1].getLong(0);
        if (rows_updated == 0) {
            throw new VoltAbortException("Failed to update a row in " + TM1Constants.TABLENAME_SPECIAL_FACILITY);
        }

        // System.err.println("UpdateSubscriberData Results:\n" +
        // Arrays.toString(results));
        return results[0].asScalarLong();
    }
}