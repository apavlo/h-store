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
    partitionParam = 0,
    singlePartition = false
)
public class DeleteCallForwarding extends VoltProcedure {

    public final SQLStmt query = new SQLStmt(
        "SELECT s_id " +
        "  FROM " + TM1Constants.TABLENAME_SUBSCRIBER + 
        " WHERE sub_nbr = ?"
    );

    public final SQLStmt update = new SQLStmt(
        "DELETE FROM " + TM1Constants.TABLENAME_CALL_FORWARDING + 
        " WHERE s_id = ? AND sf_type = ? AND start_time = ?"
    );

    public long run(String sub_nbr, long sf_type, long start_time) {
        voltQueueSQL(query, sub_nbr);
        VoltTable results[] = voltExecuteSQL();
        assert (results.length == 1);
        if (results[0].getRowCount() != 1) {
            String msg = "Got back " + results[0].getRowCount() + " tuples returned for SUB_NBR '" + sub_nbr + "'";
            throw new VoltAbortException(msg);
        }
        // System.err.println(VoltTableUtil.format(results[0]));
        boolean adv = results[0].advanceRow();
        assert (adv);
        long s_id = results[0].getLong(0);
        
        voltQueueSQL(update, s_id, sf_type, start_time);
        results = voltExecuteSQL(true);
        // System.err.println(VoltTableUtil.format(results[0]));
        assert (results.length == 1) : "Failed to delete " + TM1Constants.TABLENAME_CALL_FORWARDING + " record " + "[sub_nbr=" + sub_nbr + ",s_id=" + s_id + "]\n" + Arrays.toString(results);
        adv = results[0].advanceRow();
        assert (adv);
        long rows_updated = results[0].getLong(0);
        if (rows_updated == 0) {
            throw new VoltAbortException("Failed to delete a row in " + TM1Constants.TABLENAME_CALL_FORWARDING);
        }

        return (rows_updated);
    }
}