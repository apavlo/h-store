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
 * This procedure does an SP read, then if it hasn't found what it wants, does
 * an MP read followed by an MP write. If it found what it wanted with the SP
 * read, then it does a SP write.
 */

public class SinglePartitionWrite extends VoltProcedure {
    // Should be SP
    public final SQLStmt readA = new SQLStmt("SELECT A_IATTR00 FROM " + MarkovConstants.TABLENAME_TABLEA + " WHERE A_ID = ? ");

    // Should be MP
    public final SQLStmt findA = new SQLStmt("SELECT A_ID FROM " + MarkovConstants.TABLENAME_TABLEA + " WHERE A_IATTR00 = ? ");

    // Should be multi-partition and single-partition
    public final SQLStmt writeC = new SQLStmt("UPDATE " + MarkovConstants.TABLENAME_TABLEC + " SET C_IATTR00 = ? WHERE C_A_ID = ? ");

    public VoltTable[] run(long a_id, long value) {
        voltQueueSQL(readA, a_id); // Guaranteed to be single-sited
        VoltTable a = voltExecuteSQL()[0];
        long avalue = a.getLong(0);
        // If we don't find the a value we want, we have to start looking
        if (avalue != value) {
            voltQueueSQL(findA, value);
            a = voltExecuteSQL()[0];
            a_id = a.getLong(0);
        }
        // Now that we're done with all the single partition reads
        // we continue on to the (possible) multi-partition writes

        // Get the columns of C which need this value updated and also those
        // which are C_A_ID = A_ID
        voltQueueSQL(writeC, value, a_id);
        // Depending on the result above this update will be either single-sited
        // or multi-sited
        return voltExecuteSQL();
    }
}
