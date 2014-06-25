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
        "INSERT INTO stations (station_id, location, lat, lon, num_bikes, num_docks) VALUES (?,?,?,?,?,?);"
    );

//     public final SQLStmt insertRider = new SQLStmt(
//         "INSERT INTO riders (rider_id, f_name, l_name) VALUES (?,?);"
//     );
// 
//     public final SQLStmt insertCard = new SQLStmt(
//         "INSERT INTO cards (rider_id, bank, name, num, exp, sec_code) VALUES (?,?,?,?,?,?);"
//     );


    public long run() {

        int maxBikes    = BikerStreamConstants.NUM_BIKES_PER_STATION;
        int maxStations = BikerStreamConstants.NUM_STATIONS;

        for (int i = 0; i < maxStations; ++i){
            // Insert the Station
            voltQueueSQL(insertStation, i, "", 0.0, 0.0, 10, 0);
            voltExecuteSQL();
        }

        return 0;
    }

}
