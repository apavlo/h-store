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
public class GetDRecord extends VoltProcedure {

    public final SQLStmt GET_D = new SQLStmt("SELECT D_ID, D_B_ID, D_C_ID " + "  FROM " + MarkovConstants.TABLENAME_TABLED + " WHERE D_ID = ? ");

    public final SQLStmt GET_B = new SQLStmt("SELECT B_ID, B_A_ID, A_SATTR02, A_IATTR02 " + "  FROM " + MarkovConstants.TABLENAME_TABLEB + ", " + MarkovConstants.TABLENAME_TABLEA + " WHERE B_ID = ? "
            + "   AND B_A_ID = A_ID");

    public final SQLStmt GET_C = new SQLStmt("SELECT C_ID, C_A_ID, A_SATTR01, A_IATTR01 " + "  FROM " + MarkovConstants.TABLENAME_TABLEC + ", " + MarkovConstants.TABLENAME_TABLEA + " WHERE C_ID = ? "
            + "   AND C_A_ID = A_ID");

    public VoltTable[] run(long d_id) throws VoltAbortException {
        voltQueueSQL(GET_D, d_id);

        final VoltTable[] d_results = voltExecuteSQL();
        assert (d_results.length == 1);

        while (d_results[0].advanceRow()) {
            System.err.print("RESULT (");
            String add = "";
            for (int i = 0, cnt = d_results[0].getColumnCount(); i < cnt; i++) {
                System.err.print(add + d_results[0].getLong(i));
                add = ", ";
            } // FOR

            long b_id = d_results[0].getLong(1);
            long c_id = d_results[0].getLong(2);

            if (b_id > c_id) {
                voltQueueSQL(GET_B, b_id);
                System.err.print(add + " GET_B(");
            } else {
                voltQueueSQL(GET_C, c_id);
                System.err.print(add + " GET_C(");
            }
            add = "";

            final VoltTable[] inner_results = voltExecuteSQL();
            assert (inner_results.length == 0);
            while (inner_results[0].advanceRow()) {
                for (int i = 0, cnt = inner_results[0].getColumnCount(); i < cnt; i++) {
                    VoltType col_type = inner_results[0].getColumnType(i);
                    System.err.print(add + col_type.name() + "[");
                    System.err.print(inner_results[0].get(i, col_type));
                    System.err.print("]");
                    add = ", ";
                } // FOR
            } // WHILE
            System.err.println("))");
            return (inner_results);
        } // WHILE
        return (null);
    }
}
