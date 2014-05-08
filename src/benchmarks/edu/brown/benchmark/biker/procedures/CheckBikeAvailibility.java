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

package edu.brown.benchmark.biker.procedures;

import org.voltdb.ProcInfo;
import org.voltdb.SQLStmt;
import org.voltdb.VoltProcedure;
import org.voltdb.VoltTable;
import org.voltdb.VoltType;

import edu.brown.benchmark.biker.BikerConstants;

//import org.voltdb.types.TimestampType;

//
// Check to see that a given dock contains a bike or not
//
// -1  => Invalid dock_id
//  0  => Empty Dock
//  1  => Non-Empty Dock
//

public class CheckBikeAvailibility extends VoltProcedure {

    // check that a bike exists at the specified dock_id
    public final SQLStmt checkIfBikeIsAvailible = new SQLStmt(
        "SELECT bike_id FROM docks WHERE dock_id = ?;"
    );

    public long run(long dockId) {

        // Execute Bike Check Query
        voltQueueSQL(checkIfBikeIsAvailible, dockId);
        VoltTable results = voltExecuteSQL(true)[0];

        // Make sure the dock_id is valid
        if (results.getRowCount() == 0) {
          return BikerConstants.NOT_A_DOCK;
        }

        // Make sure dock is not empty
        if ((results.getRowCount() == 1) &&
            (results.fetchRow(0).getLong(0) == VoltType.NULL_BIGINT )) {
            return BikerConstants.DOCK_EMPTY;
        }

        // Dock is Non-empty
        return BikerConstants.DOCK_FULL;
    }
}
