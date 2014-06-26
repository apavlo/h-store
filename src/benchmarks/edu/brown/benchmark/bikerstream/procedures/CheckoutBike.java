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

public class CheckoutBike extends VoltProcedure {

    // Logging Information
    private static final Logger Log = Logger.getLogger(ReserveBike.class);
    // Is debugging on or not?
    final boolean debug = Log.isDebugEnabled();

    // Get the reservation information from given a rider_id
    public final SQLStmt checkReservation = new SQLStmt(
                "SELECT * FROM bikes WHERE last_rider = ? AND state = 'RESERVED';"
            );

    // Invalidate a previous reservation in the reservation table
    public final SQLStmt invalidateReservation = new SQLStmt(
                "UPDATE bikeres SET valid=0 WHERE rider_id = ? AND bike_id = ?;"
            );

    // Modify the state of the bike to reflect that it has been checked out
    public final SQLStmt changeBikeState = new SQLStmt(
                "UPDATE bikes " +
                "SET state = 'OUT', last_rider = ? " +
                "WHERE bike_id = ?;"
            );

    // Get all availible bikes for checkout without a reservation
    public final SQLStmt getAllAvailibleBikes = new SQLStmt(
                "SELECT * FROM bikes WHERE state = 'DOCKED';"
            );

    // Flip the occupied bit in the docks table
    public final SQLStmt removeFromDock = new SQLStmt(
                "UPDATE docks SET occupied = 0 WHERE dock_id = ?;"
            );


    public VoltTable [] run(long rider_id) {

        // Lets pull up our reservation
        voltQueueSQL(checkReservation, rider_id);
        VoltTable[] results = voltExecuteSQL();

        if (results[0].getRowCount() == 1) { // Found a Reservation
            // We have found our reservation, from here it should be easy to find and
            // grab our bike, then set everything back so that others may use our dock
            // to dock a bike.

            long bike_id = results[0].fetchRow(0).getLong("bike_id");
            long dock_id = results[0].fetchRow(0).getLong("last_dock");

            // Change the reservation table and invalidate the reservation
            voltQueueSQL(invalidateReservation, rider_id, bike_id);

            // Change the status of the bike to riding
            voltQueueSQL(changeBikeState, rider_id, bike_id);

            // Change the dock occupied status
            voltQueueSQL(removeFromDock, dock_id);

            return voltExecuteSQL(true);
            //return BikerStreamConstants.BIKE_CHECKEDOUT;


        } else if (results[0].getRowCount() == 0) { // No reservation on file (anymore?)
            // Here we have tp checkout a bike without having a reservation first
            // This is going to be tricky cause who knows if someone else will take
            // a bike out from under us in the middle of the process

            int attempts = 0;

            do { // Try to checkout a bike a few times

                voltQueueSQL(getAllAvailibleBikes);
                results = voltExecuteSQL();

                // Make sure there are bikes available
                int numOfBikes = results[0].getRowCount();

                if (numOfBikes > 0 ){
                    // Generate an index for one of the bikes in the returned
                    // table.
                    int  idx     = (int) (Math.random() * (float) numOfBikes);

                    // Return the id of an availible bike
                    long bike_id = results[0].fetchRow(idx).getLong("bike_id");
                    long dock_id = results[0].fetchRow(idx).getLong("last_dock");

                    // Update the state of the bike reflecting the 'checkout'
                    voltQueueSQL(changeBikeState, rider_id, bike_id);

                    // Change the dock occupied status
                    voltQueueSQL(removeFromDock, dock_id);

                    //results = voltExecuteSQL(true);

                    return voltExecuteSQL(true);

                    //return BikerStreamConstants.BIKE_CHECKEDOUT;
                }

            } while (attempts++ < BikerStreamConstants.MAX_CHECKOUT_ATTEMPTS);


        } else { // FATAL WEIRDNESS (must have multiple bikes reserved)
            throw new RuntimeException(
                    "[Checkout] Rider: " + rider_id +
                    " had a reservation Error (multiple reservations?)"
                    );
        }

        //return BikerStreamConstants.BIKE_NOT_CHECKEDOUT;

        return null;
    }

} // End Class

