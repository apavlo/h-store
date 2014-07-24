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


package edu.brown.benchmark.bikerstream.procedures;

import edu.brown.benchmark.bikerstream.BikerStreamConstants;
import org.apache.log4j.Logger;
import org.voltdb.*;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * This VoltProcedure will trigger on INSERT INTO bikeStatus STREAM and performs the following;
 *   a.  Replaces the TOP N near by stations for a given user in the nearByStations TABLE
 *   b.  Feeds the user_id to s1 STREAM.
 */
public class UpdateNearByStations extends VoltProcedure {
    private static final Logger LOG = Logger.getLogger(UpdateNearByStations.class);

    protected void toSetTriggerTableName() {
        addTriggerTable("bikeStatus");
    }

    public final SQLStmt getBikeCoordinate = new SQLStmt(
            "SELECT user_id, latitude, longitude FROM bikeStatus;"
    );

    public final SQLStmt getAllStationsCoordinate = new SQLStmt(
            "SELECT station_id, latitude, longitude FROM stations;"
    );

    public final SQLStmt clearUserNearByStations = new SQLStmt(
            "DELETE FROM NearByStations WHERE user_id = ?;"
    );

    public final SQLStmt insertNearByStations = new SQLStmt(
            "INSERT INTO NearByStations (user_id, station_id) VALUES (?, ?);"
    );

    public final SQLStmt feedS1Stream = new SQLStmt(
            "INSERT INTO s1 (user_id) SELECT user_id FROM bikeStatus;"
    );

    public final SQLStmt getNearByStations = new SQLStmt(
            "SELECT user_id, station_id FROM nearByStations WHERE user_id = ?;"
    );

    public final SQLStmt removeUsedBikeStatusTuple = new SQLStmt(
            "DELETE FROM bikeStatus;"
    );

    public long run() {
        //LOG.debug(" >>> Start running " + this.getClass().getSimpleName());
        // Get a handle on the new tuple
        voltQueueSQL(getBikeCoordinate);
        voltQueueSQL(getAllStationsCoordinate);
        VoltTable coordinates[] = voltExecuteSQL();
        long user_id = coordinates[0].fetchRow(0).getLong("user_id");
        double bikeLat = coordinates[0].fetchRow(0).getDouble("latitude");
        double bikeLon = coordinates[0].fetchRow(0).getDouble("longitude");

        // Clear existing data based on the new tuple
        voltQueueSQL(clearUserNearByStations, user_id);

        // Computer the distance to each stations.
        long currStation_id = -1;
        double currLat = 0.0;
        double currLon = 0.0;
        double currDist = 0.0;
        List<StationDistancePair> stationDistances = new ArrayList<StationDistancePair>();
        for (int i = 0; i < coordinates[1].getRowCount(); i++) {
            currStation_id = coordinates[1].fetchRow(i).getLong("station_id");
            currLat = coordinates[1].fetchRow(i).getDouble("latitude");
            currLon = coordinates[1].fetchRow(i).getDouble("longitude");
            currDist = (((bikeLat - currLat) * (bikeLat - currLat)) + ((bikeLon - currLon) * (bikeLon - currLon)));
            stationDistances.add(new StationDistancePair(currStation_id, currDist));
        }
        Collections.sort(stationDistances);

        // Insert the new data
        for (int j = 0; j < BikerStreamConstants.N_NEAR_BY_STATIONS && j < stationDistances.size(); j++) {
            voltQueueSQL(insertNearByStations, user_id, stationDistances.get(j).getStation_id());
            voltExecuteSQL();
        }

        /*
        // For verification purpose
        voltQueueSQL(getNearByStations, user_id);
        LOG.info("Replaced near by station for user_id " + user_id + ": " + voltExecuteSQL()[0]);
        */

        // Feed S1 after NearByStations has been updated
        voltQueueSQL(feedS1Stream);
        voltExecuteSQL();

        voltQueueSQL(removeUsedBikeStatusTuple);
        voltExecuteSQL(true);

        LOG.info(" <<< Finished running " + this.getClass().getSimpleName() + " for rider: " + user_id);
        return 0;
    }

    /**
     * Helper class to identify the top N Stations by distances in a list of StationDistancePair.
     */
    private class StationDistancePair implements Comparable<StationDistancePair> {
        private final long station_id;
        private final double distance;

        public long getStation_id() {
            return station_id;
        }

        public double getDistance() {
            return distance;
        }

        public StationDistancePair(long station_id, double distance) {
            this.station_id = station_id;
            this.distance = distance;
        }

        @Override
        public String toString() {
            return "StationDistancePair{" +
                    "station_id=" + station_id +
                    ", distance=" + distance +
                    '}';
        }

        @Override
        public int compareTo(StationDistancePair other) {
            if (this.distance > other.distance) {
                return 1;
            } else {
                return -1;
            }
        }
    }
}
