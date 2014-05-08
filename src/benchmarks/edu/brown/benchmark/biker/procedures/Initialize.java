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
// Initializes the database, pushing the list of contestants and documenting domain data (Area codes and States).
//

package edu.brown.benchmark.biker.procedures;

import org.voltdb.ProcInfo;
import org.voltdb.SQLStmt;
import org.voltdb.VoltProcedure;
import org.voltdb.VoltType;

@ProcInfo (
singlePartition = false
)
public class Initialize extends VoltProcedure
{
    // Check if the database has already been initialized
    public final SQLStmt checkStmt = new SQLStmt("SELECT COUNT(*) FROM stations;");

    // Inserts a station
    public final SQLStmt insertAStationStmt = new SQLStmt("INSERT INTO stations VALUES (?,?,?,?);");

    // Inserts a dock
    public final SQLStmt insertADockStmt = new SQLStmt("INSERT INTO docks VALUES (?,?,?);");


    // Domain data: Locations of each station and their matching coordinates
    // and Docks availability.
    // locations
    public static final String[] locations = new String[] {
        "Portland","Cambridge","Fremont","Milwaukie","Tualitin","Salem","Hilsboro","Clackamas","Boring"};

    // Station Coordinates
    public static final double[] lats = new double[] {
        45.5200, 42.3736, 37.5483, 45.5200, 42.3736, 37.5483, 45.5200, 42.3736, 37.5483};
    public static final double[] lons = new double[] {
        -122.6819, -71.1106, -121.9886, -122.6819, -71.1106, -121.9886, -122.6819, -71.1106, -121.9886 };

    // Number of available docks at a station
    public static final int numDocks[] = { 10, 13, 24, 31, 11, 22, 14, 20, 25 };

    // Number of bikes at each station
    public static final int numBikes[] = { 7, 10, 18, 20, 11, 20, 13, 15, 20 };

    // Temporary
    public static final int stationSize = 10;



    //      public static final int[] stationids = new int[]{1,2};


    // Domain data: matching lists of dockids and their
    // associated stationids and bikeids of bikes in the
    // docks
    //      public static final int[] dockids = new int[]{100,101,200,201};

    //      public static final int[] dockstationids = new int[]{1,1,2,2};

    //      public static final int[] bikeids = new int[] {1001, 1002, 1003, 1004};


    public long run() { // int maxContestants, String contestants) {

        voltQueueSQL(checkStmt);
        long existingStationCount = voltExecuteSQL()[0].asScalarLong();

        // if the data is initialized, return the contestant count
        if (existingStationCount != 0)
            return existingStationCount;

        // Load each station
        for (int i=0; i < locations.length; ++i){

            // create station id based on the index into locations
            int stationID = (i+1)*100;

            // Add the station
            voltQueueSQL(insertAStationStmt, stationID, locations[i], lats[i], lons[i]);
            voltExecuteSQL();

            // Load each dock and bike for each dock in a station
            for (int j=0; j < numDocks[i]; j++) {

                long bikeID = VoltType.NULL_BIGINT;

                if ( j < numBikes[i])
                    bikeID = (numDocks[i] * i) + j;

                voltQueueSQL(insertADockStmt, stationID+j, bikeID, stationID);
                voltExecuteSQL();
            }
        }


        voltQueueSQL(checkStmt);
        long endingStationCount = voltExecuteSQL()[0].asScalarLong();
        return endingStationCount;
    }
}
