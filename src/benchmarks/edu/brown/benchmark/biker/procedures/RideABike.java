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

import org.apache.log4j.Logger;
import org.voltdb.ProcInfo;
import org.voltdb.SQLStmt;
import org.voltdb.VoltProcedure;
import org.voltdb.VoltTable;
import org.voltdb.VoltType;
import org.voltdb.types.TimestampType;

import edu.brown.benchmark.biker.BikerConstants;
import edu.brown.utils.ThreadUtil;
import edu.brown.utils.MathUtil;

public class RideABike extends VoltProcedure {

    private static final Logger LOG = Logger.getLogger(RideABike.class);

    public static long STALL_TIME = 10; // Amount of time to wait for stalls()
    public long initialDock;      // Keep track of the initial dock from where the
                                  // bike originates
    public long finalDock;        // Save the final dock
    public long bikeID = 0;       // Keep track of the bike ID

    final boolean debug = true; //LOG.isDebugEnabled();


    // Make sure the dock has a bike given a dock_id
    public final SQLStmt getAllAvailibleBikes = new SQLStmt(
        "SELECT * FROM docks WHERE bike_id > 0;" //Hack for 'NOT NULL'
    );

    // Retreive all open docks for bike returning
    public final SQLStmt getAllAvailibleDocks = new SQLStmt(
        "SELECT * FROM docks WHERE bike_id < 0;" //Hack for 'NULL'
    );

    // Make a reservation for a bike, to be picked up in a few minutes
    // requires dock_id and a timestamp
    public final SQLStmt makeBikeReservation = new SQLStmt(
        "INSERT INTO reservations VALUES (?,1,?);"
    );

    // Make a Reservation for a dock, for the thread that already has a
    // bike checked out.
    public final SQLStmt makeDockReservation = new SQLStmt(
        "INSERT INTO reservations VALUES (?,0,?);"
    );

    public final SQLStmt removeReservation = new SQLStmt(
        "DELETE FROM reservations WHERE dock_id = ?"
    );

    // Check for reservations on the bike given a dock_id
    public final SQLStmt getReservation = new SQLStmt(
        "SELECT count(*) from reservations WHERE dock_id = ?;"
    );

    // Remove a bike from a given dock id
    // Requires:
    //      dockID
    public final SQLStmt checkoutBike = new SQLStmt(
        "UPDATE docks set bike_id = " + VoltType.NULL_BIGINT +
        " WHERE dock_id = ?;"
    );

    // Check a bike back into a dock
    // Reuires:
    //      bikeID
    //      dockID
    public final SQLStmt checkinBike = new SQLStmt(
        "UPDATE docks set bike_id = ? WHERE dock_id = ?;"
    );


    // Spend some sime spinning around town
    public long stall() {
        ThreadUtil.sleep(STALL_TIME);
        return 0;
    }

    public long reserveBike(){

        // Store My results
        VoltTable result[];

        // Added to quell unlucky dock reservers
        int attempts = 0;

        // We need to first find a bike that is docked without a reservation,
        // and put in a reservation for the bike.

        do {

            // First get a list of all availible bikes.
            voltQueueSQL(getAllAvailibleBikes);
            result = voltExecuteSQL();

            // Make sure there are bikes availible
            int numOfBikes = result[0].getRowCount();
            if (numOfBikes < 1){
                return BikerConstants.NO_BIKES_AVAILIBLE;
            }

            if (attempts++ > 5)
                return BikerConstants.NO_BIKES_AVAILIBLE;

            // Generate an index for one of the bikes in the returned
            // table.
            int idx = (int) (Math.random() * (float) numOfBikes);

            // Using our random index, we'll go retrieve a dock_id and just
            // stick it into an initialDock variable.
            initialDock = result[0].fetchRow(idx).getLong(0);

            // We should also keep track of the bike_id, so that we can return
            // it correctly later.
            bikeID = result[0].fetchRow(idx).getLong(1);

            // get the current time, this way we can expire reservations over
            //  900000 miliseconds (15 Minutes)
            TimestampType ts = new TimestampType();

            // Reserve the bike/dock
            voltQueueSQL(makeBikeReservation, initialDock, ts);
            result = voltExecuteSQL();

        } while (result[0].getRowCount() != 1);

        // Do we still have a hole of that bikeID? then we got the reservation.
        // BOOYEAH, lets quit reserving bikes and go pick it up.
        return BikerConstants.BIKE_RESERVED;
    }


    public long checkoutBike(){

        // I arrive at the station
        // So lets checkout that bike.
        voltQueueSQL(checkoutBike, initialDock);
        voltQueueSQL(removeReservation, initialDock);
        VoltTable result[] = voltExecuteSQL();
        if (result[0].getRowCount() != 1)
            return BikerConstants.CHECKOUT_ERROR;
        return BikerConstants.CHECKOUT_SUCCESS;

    }


    public long reserveDock(){

        // Store My results
        VoltTable result[];

        // Now it's time to pick and empty dock and reserve it
        do {
            // First get a list of all availible docks.
            voltQueueSQL(getAllAvailibleDocks);
            result = voltExecuteSQL();

            // Make sure there are docks availible
            int numOfDocks = result[0].getRowCount();
            if (numOfDocks < 1)
                return BikerConstants.NO_DOCKS_AVAILIBLE;

            // Generate an index for one of the docks in the returned
            // table.
            int idx = (int) (Math.random() * (float) numOfDocks);

            // Using our random index, we'll go retrieve a dock_id and just
            // stick it into an finalDock variable.
            finalDock = result[0].fetchRow(idx).getLong(0);

            // get the current time, this way we can expire reservations over
            //  900000 miliseconds (15 Minutes)
            TimestampType ts = new TimestampType();

            // Reserve the bike/dock
            voltQueueSQL(makeDockReservation, initialDock, ts);
            result = voltExecuteSQL();

            // In case of Races, if some other thread gets the reservation first,
            // we'll go for another bike
        } while (result[0].getRowCount() != 1);
        return BikerConstants.DOCK_RESERVED;
    }


    public long checkinBike(){
        // Now it's time to dock the bike
        voltQueueSQL(checkinBike, bikeID, finalDock);
        voltQueueSQL(removeReservation, finalDock);
        VoltTable result[] = voltExecuteSQL(true);
        if (result[0].asScalarLong() != 1)
            return BikerConstants.CHECKIN_ERROR;
        return BikerConstants.CHECKIN_SUCCESS;
    }


    // Reserve the dock_id if there is a bike present
    // and No prior reservation exists
    public long run() {

        if (reserveBike() == BikerConstants.NO_BIKES_AVAILIBLE)
            throw new VoltAbortException("Could not get a bike reservation fast enough");

        checkoutBike();

        reserveDock();

        checkinBike();

        return BikerConstants.RIDE_SUCCESS;

    }
}
