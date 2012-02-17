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
 * This method aborts if it receives no results. It will update B or C and D
 * with a new value if it retrieves a new value. Otherwise it will update C and
 * D with the passed in value and abort.
 * 
 * @author svelagap
 */
public class UserAbort extends VoltProcedure {
    public final SQLStmt writeB = new SQLStmt("UPDATE " + MarkovConstants.TABLENAME_TABLEB + " SET B_IATTR01 = ? WHERE B_A_ID  = ?");
    public final SQLStmt writeC = new SQLStmt("UPDATE " + MarkovConstants.TABLENAME_TABLEC + " SET C_IATTR01 = ? WHERE C_A_ID  = ?");
    public final SQLStmt writeD = new SQLStmt("UPDATE " + MarkovConstants.TABLENAME_TABLED + " SET D_IATTR01 = ? WHERE D_C_A_ID  = ?");
    public final SQLStmt fromA = new SQLStmt("SELECT " + " A_IATTR00 FROM " + MarkovConstants.TABLENAME_TABLEA + " WHERE A_ID = ? AND A_IATTR01 = ?");

    public VoltTable[] run(long a_id, long value) throws VoltAbortException {
        long newvalue = 0;
        voltQueueSQL(fromA, a_id, value);
        // (SP) Necessarily single partition. We retrieve the attribute we want
        // (IATTR00)
        VoltTable[] tables = voltExecuteSQL();
        // If there was nothing returned, write to the tables on the local
        // partition for C and D
        // and then abort. This means that when we see a B being written to in
        // the trace, our graph will
        // know that the abort probability is 0
        if (tables == null || tables.length == 0) {
            voltQueueSQL(writeC, value, a_id);
            voltQueueSQL(writeD, value, a_id);
            voltExecuteSQL();
            throw new VoltAbortException("No results returned");
        }
        // If we didn't abort, write to B with some probability, C/D with some
        // other probability
        while (tables[0].advanceRow()) {
            newvalue = tables[0].getLong(0);
        }
        if (newvalue > 5) {
            voltQueueSQL(writeB, newvalue, a_id);
        } else {
            voltQueueSQL(writeC, newvalue, a_id);
            voltQueueSQL(writeD, newvalue, a_id);
        }
        return voltExecuteSQL();
    }
}
