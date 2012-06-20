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

import org.voltdb.SQLStmt;
import org.voltdb.VoltProcedure;
import org.voltdb.VoltTable;

import edu.brown.benchmark.markov.MarkovConstants;

/**
 * This procedure creates many branches in the MarkovGraph - each one with a
 * specific number of iterations through the while loop.
 * 
 * @author svelagap
 */
public class ExecutionTime extends VoltProcedure {

    // Should be single partition
    public final SQLStmt selectA = new SQLStmt("SELECT A_IATTR02 FROM " + MarkovConstants.TABLENAME_TABLEA + " WHERE A_ID = ? ");

    // ???
    public final SQLStmt updateA = new SQLStmt("UPDATE " + MarkovConstants.TABLENAME_TABLEA + " SET A_IATTR02 = A_IATTR02 + ? WHERE A_ID = ?");

    // Should be multi-partition
    public final SQLStmt updateC = new SQLStmt("UPDATE " + MarkovConstants.TABLENAME_TABLEC + " SET C_IATTR02 = ? WHERE C_A_ID = ? ");

    // Should be single-partition
    public final SQLStmt updateB = new SQLStmt("UPDATE " + MarkovConstants.TABLENAME_TABLEB + " SET B_IATTR02 = B_IATTR02 + ? WHERE B_A_ID = ? ");

    public VoltTable[] run(long a_id) {
        voltQueueSQL(selectA, a_id);
        // (SP) Read a single attribute value from the current partition
        long writeToB = voltExecuteSQL()[0].getLong(0);
        if (writeToB > 5) {
            while (writeToB > 5) {
                // depending on the value returned from A, we iterate through
                // and increment B
                // a silly way to do this, yes, but this is science!
                voltQueueSQL(updateB, writeToB, a_id);
                voltExecuteSQL();
                writeToB--;
            }
        } else {
            // Increment this attribute in A
            voltQueueSQL(updateA, 1, a_id);
        }
        return voltExecuteSQL();
    }

}
