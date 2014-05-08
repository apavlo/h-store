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
import org.voltdb.types.TimestampType;

import edu.brown.benchmark.biker.BikerConstants;


public class ReserveBike extends VoltProcedure {

    // Make sure the dock has a bike
    // Given a dock_id
    public final SQLStmt checkForBike = new SQLStmt(
        "SELECT count(*) FROM docks WHERE dock_id = ?;" //  AND bike_id IS NOT NULL;"
    );

    // Check for reservations on the bike
    // Given a dock_id
    public final SQLStmt checkReservation = new SQLStmt(
        "SELECT count(*) from reservations WHERE dock_id = ?;"
    );

    // Insert a reservation for a specified dock
    public final SQLStmt insertAReservation = new SQLStmt(
        "INSERT INTO reservations VALUES (?,1,?);"
    );

    public long fifteenMinutesFromNow() {
        // 25 minutes in the future given
        // 900000000 microseconds
        TimestampType ts = new TimestampType();
        return (BikerConstants.RESERVATION_DURATION + ts.getTime());
    }

    // Reserve the dock_id if there is a bike present
    // and No prior reservation exists
    public long run(long dockId) {

        voltQueueSQL(checkForBike, dockId);
        voltQueueSQL(checkReservation, dockId);
        VoltTable result[] = voltExecuteSQL();

        // Check that the dock ID has a bike
        if (result[0].getRowCount() != 1){
            System.out.println("1");
            return BikerConstants.DOCK_UNAVAILIBLE;
        }

        // Check that the dock ID has a bike
        if (result[1].getRowCount() > 0){
            System.out.println("2");
            return BikerConstants.DOCK_UNAVAILIBLE;
        }

        System.out.println("3");
        voltQueueSQL(insertAReservation, dockId, fifteenMinutesFromNow());
        result = voltExecuteSQL();
        return BikerConstants.BIKE_RESERVED;

    }
}
