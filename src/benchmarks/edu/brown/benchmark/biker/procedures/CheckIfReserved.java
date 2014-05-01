/***************************************************************************
 *  Copyright (C) 2014 by H-Store Project                                  *
 *  Brown University                                                       *
 *  Massachusetts Institute of Technology                                  *
 *  Yale University                                                        *
 *  Portland State University                                              *
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

//
// Checks if a dock (identified by dock_id) is reserved or
// not

package edu.brown.benchmark.biker.procedures;

import org.voltdb.ProcInfo;
import org.voltdb.SQLStmt;
import org.voltdb.VoltProcedure;
import org.voltdb.VoltTable;
import org.voltdb.types.TimestampType;

public class CheckIfReserved extends VoltProcedure {

    // Checks if the vote is for a valid contestant
    public final SQLStmt checkIfReserved = new SQLStmt(
        "SELECT count(*) FROM reservations WHERE dock_id = ?;"
    );

    // Records a vote
    //public final SQLStmt insertVoteStmt = new SQLStmt(
        //"INSERT INTO votes (vote_id, phone_number, state, contestant_number, created) VALUES (?, ?, ?, ?, ?);"
    //);

    // returns 1 if reserved, 0 if not, -1 on error
    // I know this return makes no sense, but it's a
    // start...
    public long run(long dockId) {

        // Queue up validation statements
        voltQueueSQL(checkIfReserved, dockId);
        VoltTable validation[] = voltExecuteSQL();

        if (validation[0].getRowCount() == 0) {
          return -1;
        }

        if ((validation[1].getRowCount() == 1) &&
            (validation[1].asScalarLong() == 1)) {
            return 1;
        }
        else {
            return 0;
        }

    }
}
