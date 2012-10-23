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

import org.voltdb.ProcInfo;
import org.voltdb.SQLStmt;
import org.voltdb.VoltProcedure;
import org.voltdb.VoltTable;

import edu.brown.benchmark.tm1.TM1Constants;

@ProcInfo(
    partitionParam = 0,
    singlePartition = false
)
public class InsertCallForwarding extends VoltProcedure {
    public final SQLStmt query1 = new SQLStmt(
        "SELECT s_id FROM " + TM1Constants.TABLENAME_SUBSCRIBER + " WHERE sub_nbr = ?"
    );

    public final SQLStmt query2 = new SQLStmt(
        "SELECT sf_type FROM " + TM1Constants.TABLENAME_SPECIAL_FACILITY + " WHERE s_id = ?"
    );

    public final SQLStmt update = new SQLStmt(
        "INSERT INTO " + TM1Constants.TABLENAME_CALL_FORWARDING + " VALUES (?, ?, ?, ?, ?)"
    );

    public final SQLStmt check = new SQLStmt(
        "SELECT s_id FROM " + TM1Constants.TABLENAME_CALL_FORWARDING +
        " WHERE s_id = ? AND sf_type = ? AND start_time = ?"
    );

    /**
     * If this is set to true, then we will use the "check" query to look
     * whether our Call_Forwarding record already exists before we try to insert
     * it.
     */
    public static final boolean NO_CONSTRAINT_ABORT = true;

    public long run(String sub_nbr, long sf_type, long start_time, long end_time, String numberx) throws VoltAbortException {
        VoltTable result[] = null;
        voltQueueSQL(query1, sub_nbr);
        result = voltExecuteSQL();
        assert (result.length == 1);
        if (result[0].getRowCount() != 1) {
            String msg = "Got back " + result[0].getRowCount() + " tuples returned for sub_nbr '" + sub_nbr + "'";
            throw new VoltAbortException(msg);
        }
        boolean adv = result[0].advanceRow();
        assert (adv);
        long s_id = result[0].getLong(0);

        voltQueueSQL(query2, s_id);
        voltExecuteSQL();

        // System.err.println("INSERT CALL FORWARD SLEEP!");
        // ThreadUtil.sleep(10000);

        // Inserting a new Call_Forwarding record only succeeds 30% of the time
        // Check whether the record that we want to insert already exists in
        // Call_Forwarding
        // This is *not* how it's suppose to be done, but there is current no
        // way to check
        // whether the insert failed from inside of the xact
        if (NO_CONSTRAINT_ABORT) {
            voltQueueSQL(check, s_id, sf_type, start_time);
            result = voltExecuteSQL(true);
        }
        if (result == null || result.length == 0 || result[0].getRowCount() == 0) {
            voltQueueSQL(update, s_id, sf_type, start_time, end_time, numberx);
            result = voltExecuteSQL(true);
            assert result.length == 1;
            return result[0].asScalarLong();
        }
        if (NO_CONSTRAINT_ABORT) {
            throw new VoltAbortException("Call_Forwarding entry already exists");
        }
        return (0l);
    }
}