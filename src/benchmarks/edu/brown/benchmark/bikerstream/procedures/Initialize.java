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

    public final SQLStmt insertZone = new SQLStmt(
        "INSERT INTO zones (zone_id, discount) VALUES (?,?);"
    );

    public final SQLStmt insertStation = new SQLStmt(
        "INSERT INTO stations (station_id, zone_id, location, lat, lon) VALUES (?,?,?,?,?);"
    );

    public final SQLStmt insertBike = new SQLStmt(
        "INSERT INTO bikes (bike_id, zone_id, condition, state, last_dock, last_rider)" +
        " VALUES (?,?,?,?,?,?);"
    );

    public final SQLStmt insertDock = new SQLStmt(
        "INSERT INTO docks (dock_id, station_id, occupied) VALUES (?,?,?);"
    );

    public final SQLStmt fillDock = new SQLStmt(
        "UPDATE docks set occupied = 1 WHERE dock_id = ?;"
    );

//     public final SQLStmt insertRider = new SQLStmt(
//         "INSERT INTO riders (rider_id, f_name, l_name) VALUES (?,?);"
//     );
// 
//     public final SQLStmt insertCard = new SQLStmt(
//         "INSERT INTO cards (rider_id, bank, name, num, exp, sec_code) VALUES (?,?,?,?,?,?);"
//     );


    public long run() {

        assert(BikerStreamConstants.NUM_BIKES_PER_STATION <= BikerStreamConstants.NUM_DOCKS_PER_STATION);

        for (int i=0; i < BikerStreamConstants.NUM_ZONES; ++i){

            int zone_id = i * 100000;

            // Insert the zone
            voltQueueSQL(insertZone, zone_id, 1.0);
            voltExecuteSQL();

            for (int j=0; j< BikerStreamConstants.NUM_STATIONS_PER_ZONE; ++j){

                int station_id = zone_id + (j*1000);

                // Insert the Station
                voltQueueSQL(insertStation, station_id, zone_id, "", 0.0, 0.0);
                voltExecuteSQL();

                // Insert docks and bikes
                for (int h =0; h < BikerStreamConstants.NUM_DOCKS_PER_STATION; ++h){

                    int dock_id = station_id + (10*h);
                    int bike_id = station_id + h;


                    if (h < BikerStreamConstants.NUM_BIKES_PER_STATION){
                        voltQueueSQL(insertDock, dock_id, station_id, 1);
                        voltQueueSQL(insertBike, bike_id, zone_id, "New", "DOCKED", dock_id, -1);
                    } else {
                        voltQueueSQL(insertDock, dock_id, station_id, 0);
                    }

                    voltExecuteSQL();

                } // Docks

            } // Station

        } // Zones

        // // Insert Riders
        // for (int i=0; i < riderNames.length; ++i){
        //     voltQueueSQL(insertRider, (i*10)+i , riderNames[i]);
        //     voltQueueSQL(insertCard, (i*10) +i, "Visa", riderNames[i], String.valueOf(i), "JAN/2016", 123);
        //     voltExecuteSQL();

        // }

        return 0;
    }

}
