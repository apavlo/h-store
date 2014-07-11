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

/* Initialize - nothing to do */

package edu.brown.benchmark.bikerstream.procedures;

import edu.brown.benchmark.bikerstream.BikerStreamConstants;

import org.voltdb.ProcInfo;
import org.voltdb.SQLStmt;
import org.voltdb.VoltProcedure;
import org.voltdb.VoltType;

@ProcInfo (
singlePartition = false
)
public class Initialize extends VoltProcedure
{

    public final SQLStmt insertStation = new SQLStmt(
        "INSERT INTO stations (station_id, station_name, street_address, latitude, longitude) " +
        "VALUES (?,?,?,?,?)"
    );

    public final SQLStmt insertBike = new SQLStmt(
        "INSERT INTO bikes (bike_id, current_status) VALUES (?,?)"
    );

    public final SQLStmt initialStationStatus = new SQLStmt(
        "INSERT INTO StationStatus (station_id, current_bikes, current_docks, current_bike_discount, current_dock_discount) " +
        "VALUES (?,?,?,?,?)"

    );

    public long run() {

        int maxBikes    = BikerStreamConstants.NUM_BIKES_PER_STATION;
        int maxStations = BikerStreamConstants.STATION_LOCATIONS.length;

        for (int i = 0; i < maxStations; ++i){
            // Insert the Station
            voltQueueSQL(insertStation, i+1, BikerStreamConstants.STATION_LOCATIONS[i], "ADRESS HERE", i, i);

            int j;
            for (j = 0; j < 10; ++j) {
                voltQueueSQL(insertBike, (i*1000) + j, 0);
            }
            voltQueueSQL(initialStationStatus, i, j +1, 20 - (j+1), 0.0, 0.0);
            voltExecuteSQL();
        }

        return 0;
    }

}
