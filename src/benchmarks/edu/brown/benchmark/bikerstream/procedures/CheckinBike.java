/* This file is part of VoltDB.
 * Copyright (C) 2008-2012 VoltDB Inc.
 *
 * Permission is hereby granted, free of charge, to any person obtaining
 * a copy of this software and associated documentation files (the
 * "Software"), to deal in the Software without restriction, including
 * without limitation the rights to use, copy, modify, merge, publish,
 * distribute, sublicense, and/or sell copies of the Software, and to
 * permit persons to whom the Software is furnished to do so, subject to
 * the following conditions:
 *
 * The above copyright notice and this permission notice shall be
 * included in all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
 * IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR
 * OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
 * ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
 * OTHER DEALINGS IN THE SOFTWARE.
 */

//
// Rides a bike - basically inserts events theoretically
// from a gps on a bike into the bikerreadings_stream
//

package edu.brown.benchmark.bikerstream.procedures;

import org.apache.log4j.Logger;
import org.voltdb.ProcInfo;
import org.voltdb.SQLStmt;
import org.voltdb.VoltProcedure;
import org.voltdb.VoltTable;
import org.voltdb.VoltType;
import org.voltdb.types.TimestampType;

import edu.brown.benchmark.bikerstream.BikerStreamConstants;

@ProcInfo (
    singlePartition = true
)

public class CheckinBike extends VoltProcedure {

    // Logging Information
    private static final Logger Log = Logger.getLogger(ReserveBike.class);
    // Is debugging on or not?
    final boolean debug = Log.isDebugEnabled();

    // Check to see if there exists a reservation for the current rider/dock/bike
    public final SQLStmt checkReservation = new SQLStmt(
                "SELECT * FROM bikes WHERE last_rider = ? AND state = 'RETURNING';"
            );

    // Get the reservation information
    public final SQLStmt getReservation = new SQLStmt(
                "SELECT * FROM dockRes where rider_id = ? AND bike_id = ? AND valid = 1;"
            );


    // Remove the dock reservation foor those that have one
    public final SQLStmt invalidateReservation = new SQLStmt(
                "UPDATE dockres SET valid=0 WHERE rider_id = ? AND dock_id = ? AND valid = 1;"
            );

    // update the state of the bike to docked once we have our dock
    public final SQLStmt changeBikeState = new SQLStmt(
                "UPDATE bikes " +
                "SET state = 'DOCKED', last_dock = ? " +
                "WHERE bike_id = ?;"
            );

    // For those without a reservation, enumerate the list of open docks
    public final SQLStmt getAllAvailibleDocks = new SQLStmt(
                "SELECT * FROM docks WHERE occupied = 0;"
            );

    // update the dock information to reflect that presense of a bike
    public final SQLStmt parkInDock = new SQLStmt(
                "UPDATE docks SET occupied = 1 WHERE dock_id = ?;"
            );

    public final SQLStmt getBike = new SQLStmt(
            "SELECT bike_id FROM bikes where last_rider = ? AND state = 'OUT'"
            );


    public long run(long rider_id) {

        // Lets pull up our reservation
        voltQueueSQL(checkReservation, rider_id);
        VoltTable[] results = voltExecuteSQL();


        if (results[0].getRowCount() == 1) { // Found a Reservation

            long bike_id = results[0].fetchRow(0).getLong("bike_id");

            voltQueueSQL(getReservation, rider_id, bike_id);
            results = voltExecuteSQL();

            long dock_id = results[0].fetchRow(0).getLong("dock_id");

            // Change the reservation table and invalidate the reservation
            voltQueueSQL(invalidateReservation, rider_id, dock_id);

            // Change the status of the bike to riding
            voltQueueSQL(changeBikeState, dock_id, bike_id);

            // Change the dock occupied status
            voltQueueSQL(parkInDock, dock_id);

            voltExecuteSQL(true);
            return BikerStreamConstants.BIKE_CHECKEDIN;


        } else if (results[0].getRowCount() == 0) { // No reservation on file (anymore?)
            // TODO: ADD A TRY BLOCK

            int attempts = 0;

            do { // Try to checkin to a dock

                voltQueueSQL(getAllAvailibleDocks);
                results = voltExecuteSQL();

                // Make sure there are bikes available
                int numOfDocks = results[0].getRowCount();

                if (numOfDocks > 0 ){
                    // Generate an index for one of the bikes in the returned
                    // table.
                    int idx      = (int) (Math.random() * (float) numOfDocks);

                    long dock_id = results[0].fetchRow(idx).getLong("dock_id");

                    voltQueueSQL(getBike, rider_id);
                    results = voltExecuteSQL();

                    assert(results[0].getRowCount() > 0);

                    long bike_id = results[0].asScalarLong();

                    // Update the state of the bike reflecting the 'checkout'
                    voltQueueSQL(changeBikeState, dock_id, bike_id);

                    // Change the dock occupied status
                    voltQueueSQL(parkInDock, dock_id);

                    results = voltExecuteSQL(true);

                    return BikerStreamConstants.BIKE_CHECKEDIN;
                }

            } while (attempts++ < BikerStreamConstants.MAX_CHECKIN_ATTEMPTS);


        } else { // FATAL WEIRDNESS (must have multiple bikes reserved)
            throw new RuntimeException(
                    "[Checkout] Rider: " + rider_id +
                    " had a reservation Error (multiple reservations?)"
                    );
        }

        return BikerStreamConstants.BIKE_NOT_CHECKEDIN;
    }

} // End Class

