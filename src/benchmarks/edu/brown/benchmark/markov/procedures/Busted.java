/***************************************************************************
 *   Copyright (C) 2010 by H-Store Project                                 *
 *   Brown University                                                      *
 *   Massachusetts Institute of Technology                                 *
 *   Yale University                                                       *
 *                                                                         *
 *   Permission is hereby granted, free of charge, to any person obtaining *
 *   a copy of this software and associated documentation files (the       *
 *   "Software"), to deal in the Software without restriction, including   *
 *   without limitation the rights to use, copy, modify, merge, publish,   *
 *   distribute, sublicense, and/or sell copies of the Software, and to    *
 *   permit persons to whom the Software is furnished to do so, subject to *
 *   the following conditions:                                             *
 *                                                                         *
 *   The above copyright notice and this permission notice shall be        *
 *   included in all copies or substantial portions of the Software.       *
 *                                                                         *
 *   THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,       *
 *   EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF    *
 *   MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.*
 *   IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR     *
 *   OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, *
 *   ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR *
 *   OTHER DEALINGS IN THE SOFTWARE.                                       *
 ***************************************************************************/
package edu.brown.benchmark.markov.procedures;

import org.voltdb.ProcInfo;
import org.voltdb.SQLStmt;
import org.voltdb.VoltProcedure;
import org.voltdb.VoltTable;
import org.voltdb.VoltType;

import edu.brown.benchmark.markov.MarkovConstants;

@ProcInfo(singlePartition = false)
public class Busted extends VoltProcedure {

    public final SQLStmt FAIL_SQL = new SQLStmt("SELECT B_ID, B_A_ID, A_SATTR00 " + "  FROM " + MarkovConstants.TABLENAME_TABLEB + ", " + MarkovConstants.TABLENAME_TABLEA + " WHERE B_ID = ? "
            + "   AND B_A_ID = A_ID");

    public VoltTable[] run(long d_id) throws VoltAbortException {
        voltQueueSQL(FAIL_SQL, d_id);

        final VoltTable[] d_results = voltExecuteSQL();
        assert (d_results.length == 1);

        while (d_results[0].advanceRow()) {
            System.err.print("RESULT (");
            String add = "";
            for (int i = 0, cnt = d_results[0].getColumnCount(); i < cnt; i++) {
                VoltType col_type = d_results[0].getColumnType(i);
                System.err.print(add + col_type.name() + "[");
                System.err.print(d_results[0].get(i, col_type));
                System.err.print("]");
                add = ", ";
            } // FOR
            System.err.println(")");
        } // WHILE
        return (d_results);
    }
}
