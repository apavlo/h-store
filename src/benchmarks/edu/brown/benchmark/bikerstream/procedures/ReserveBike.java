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
    // TODO - don't know what to do about partitioning
    //partitionInfo = "votes.phone_number:1",
    singlePartition = true
)

public class ReserveBike extends VoltProcedure {

    // Logging Information
    private static final Logger Log = Logger.getLogger(ReserveBike.class);
    // Is debugging on or not?
    final boolean debug = Log.isDebugEnabled();

    /*
     *  Get a list of all docks that have a bike checked in.
     */
    public final SQLStmt getAllAvailibleBikes = new SQLStmt(
            "SELECT * FROM bikes WHERE state = 'DOCKED';" //Hack for 'NOT NULL'
            );

    /*
     *  Make a reservation for a bike, to be picked up in a few minutes,
     *  requires dock_id and a time stamp
     */
    public final SQLStmt makeBikeReservation = new SQLStmt(
            "INSERT INTO bikeRes (bike_id, dock_id, rider_id, valid, time) VALUES (?,?,?,1,?);"
            );

    /*
     *  Update the bike_state table to reflect the new state of the bike
     *  upon successfull reservation
     */
    public final SQLStmt updateBikeState = new SQLStmt(
            "UPDATE bikes SET state='RESERVED', last_rider=? WHERE bike_id = ?;"
            );


    public long run(long rider_id) {

        VoltTable results[];

        if (debug)
            Log.debug("[RIDER " + rider_id + "] Attempting to reserve a bike.");

        // Keep track of how many times we attempt to make a reservation and
        // cease to do so once we hit maximum attempts.
        int attempts = 0;

        // We need to first find a bike that is docked without a reservation,
        // and put in a reservation for the bike.
        do {

            // First get a list of all available bikes.
            voltQueueSQL(getAllAvailibleBikes);
            results = voltExecuteSQL();

            // Make sure there are bikes available
            int numOfBikes = results[0].getRowCount();
            if (numOfBikes > 0){

                // Generate an index for one of the bikes in the returned
                // table.
                int idx = (int) (Math.random() * (float) numOfBikes);

                // Using our random index, we'll go retrieve a dock_id and just
                // stick it into an initial dock variable.
                long bike_id = results[0].fetchRow(idx).getLong("bike_id");
                long dock_id = results[0].fetchRow(idx).getLong("last_dock");

                // get the current time, this way we can expire reservations over
                //  900000 milliseconds (15 Minutes)
                TimestampType current_time = new TimestampType();

                try {

                    // Reserve the bike/dock
                    voltQueueSQL(makeBikeReservation, bike_id, dock_id, rider_id, current_time);
                    voltQueueSQL(updateBikeState, rider_id, bike_id);
                    voltExecuteSQL(true);
                    return BikerStreamConstants.BIKE_RESERVED;

                } catch (Exception e) {

                    if (debug)
                        Log.debug("[RIDER " + rider_id + "] FAILED to reserved bike: " + bike_id);

                }

                // if (result[0].getRowCount() < 1 && result[1].getRowCount() == 1) {
                //     if (debug)
                //     return BikerStreamConstants.BIKE_RESERVED;
                // }

                //voltQueueSQL(checkReservation, dock_id);
            }
        } while (attempts++ < BikerStreamConstants.MAX_RESERVE_ATTEMPTS);

        if (debug)
            Log.debug("[RIDER " + rider_id + "] FAILED to reserve any bikes");

        return BikerStreamConstants.BIKE_NOT_RESERVED;
    }

} // End Class

