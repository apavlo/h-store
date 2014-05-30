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

public class ReserveDock2 extends VoltProcedure {

    // Logging Information
    private static final Logger Log = Logger.getLogger(ReserveBike.class);
    // Is debugging on or not?
    final boolean debug = Log.isDebugEnabled();

    /*
     * Get a list of all docks that have a bike checked in.
     */
    public final SQLStmt getAllAvailibleDocks = new SQLStmt(
                "SELECT dock_id, discount "+
                "FROM   docks d, stations s, zones z " +
                "WHERE  d.occupied = 0 " +
                "AND    d.station_id = s.station_id "+
                "AND    s.zone_id    = z.zone_id"
            );

    /*
     *  Make a reservation for a bike, to be picked up in a few minutes,
     *  requires dock_id and a time stamp
     */
    public final SQLStmt makeDockReservation = new SQLStmt(
            "INSERT INTO dockRes (dock_id, bike_id, rider_id, discount, valid, time) VALUES (?,?,?,?,1,?);"
            );

    /*
     *  Check for reservations on the bike given a dock_id
     */
    public final SQLStmt getBikeInfo = new SQLStmt(
            "SELECT * from bikes WHERE last_rider = ? AND state = 'RIDING';"
            );

    /*
     *  Check for reservations on the bike given a dock_id
     */
    public final SQLStmt checkReservation = new SQLStmt(
            "SELECT * from dockRes WHERE dock_id = ? AND valid = 1 ORDER BY time ASC;"
            );

    /*
     *  Go flip the occupied bit of the dock to 1 to signify that it has been taken
     */
    public final SQLStmt occupyDock = new SQLStmt(
            "UPDATE docks SET occupied = 1 WHERE dock_id = ?;"
            );

    public final SQLStmt removeReservation = new SQLStmt(
            "UPDATE dockRes SET valid = -1 WHERE dock_id = ? AND rider_id = ? AND valid = 1;"
            );

    public final SQLStmt updateState = new SQLStmt(
            "UPDATE bikes SET state = 'RETURNING' WHERE bike_id = ?;"
            );


    public long run(long rider_id) {

        if (debug)
            Log.debug("[RIDER " + rider_id + "] Attempting to reserve a dock.");

        VoltTable result[];

        voltQueueSQL(getBikeInfo, rider_id);
        result = voltExecuteSQL();

        assert(result[0].getRowCount() > 0);

        // Get our current Bike id
        long bike_id  = result[0].fetchRow(0).getLong("bike_id");


        // Keep track of how many times we attempt to make a reservation and
        // cease to do so once we hit maximum attempts.
        int attempts = 0;

        // We need to first find a bike that is docked without a reservation,
        // and put in a reservation for the bike.
        do {

            // First get a list of all available bikes.
            voltQueueSQL(getAllAvailibleDocks);
            result = voltExecuteSQL();

            // Make sure there are bikes available
            int numOfDocks = result[0].getRowCount();
            if (numOfDocks > 0){

                // Generate an index for one of the bikes in the returned
                // table.
                int idx = (int) (Math.random() * (float) numOfDocks);

                // Using our random index, we'll go retrieve a dock_id and just
                // stick it into an initial dock variable.
                long  dock_id  = result[0].fetchRow(idx).getLong("dock_id");
                double discount = result[0].fetchRow(idx).getDouble("discount");


                // get the current time
                TimestampType current_time = new TimestampType();

                try {
                    // Reserve the dock
                    voltQueueSQL(makeDockReservation, dock_id, bike_id, rider_id, discount, current_time);
                    voltQueueSQL(checkReservation, dock_id);
                    result = voltExecuteSQL();

                    if (result[1].fetchRow(0).getLong("rider_id") == rider_id) { // We got it
                        voltQueueSQL(updateState, bike_id);
                        voltExecuteSQL(true);
                        return BikerStreamConstants.DOCK_RESERVED;
                    } else {
                        // Someone else beat us to the dock
                        voltQueueSQL(removeReservation, dock_id, rider_id);
                        voltExecuteSQL();
                    }
                } catch (Exception e) {
                    if (debug)
                        Log.debug("[RIDER " + rider_id + "] FAILED to reserved dock: " + dock_id);
                }

            }

        } while (attempts++ < BikerStreamConstants.MAX_DOCK_RESERVE_ATTEMPTS);

        return BikerStreamConstants.DOCK_NOT_RESERVED;

    }

} // End Class

