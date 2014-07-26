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
        "INSERT INTO bikes (bike_id, station_id, current_status) VALUES (?,?,?)"
    );

    public final SQLStmt initialStationStatus = new SQLStmt(
        "INSERT INTO StationStatus (station_id, current_bikes, current_docks, current_discount) " +
        "VALUES (?,?,?,?)"

    );

    public long run() {

        int numBikes    = BikerStreamConstants.NUM_BIKES_PER_STATION;
        int numDocks    = BikerStreamConstants.NUM_DOCKS_PER_STATION;
        int numStations = BikerStreamConstants.STATION_NAMES.length;

        for (int i = 0; i < numStations; ++i){
            // Insert the Station
            voltQueueSQL(insertStation,
                    i,
                    BikerStreamConstants.STATION_NAMES[i],
                    "ADRESS HERE",
                    BikerStreamConstants.STATION_LATS[i],
                    BikerStreamConstants.STATION_LONS[i]);

            int j;
            for (j = 0; j < numBikes; ++j) {
                voltQueueSQL(insertBike, (i*1000) + j, i, 1);
            }
            voltQueueSQL(initialStationStatus, i, numBikes, numDocks - numBikes, 0.0);
            voltExecuteSQL();
        }

        return 0;
    }

}
