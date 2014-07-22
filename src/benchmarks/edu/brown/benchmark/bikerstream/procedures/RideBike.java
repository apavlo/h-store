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
import org.voltdb.types.TimestampType;

import edu.brown.benchmark.bikerstream.BikerStreamConstants;

@ProcInfo (
    // TODO - don't know what to do about partitioninng
    //partitionInfo = "votes.phone_number:1",
    singlePartition = true
)
public class RideBike extends VoltProcedure {

    private static final Logger Log = Logger.getLogger(RideBike.class);

    // Enters a bike ride gps event
    public final SQLStmt insertBikeReadingStmt = new SQLStmt(
        "INSERT INTO bikeStatus (user_id, latitude, longitude, time) " +
        "VALUES (?, ?, ?, ?);"
    );

    // Enters a bike ride gps event
    public final SQLStmt log = new SQLStmt(
        "INSERT INTO logs (user_id, time, success, action) " +
        "VALUES (?, ?, ?, ?);"
    );

    public long run(long rider_id, double reading_lat, double reading_lon) throws Exception {

        try {
            // Post the ride event
            TimestampType time = new TimestampType();
            voltQueueSQL(insertBikeReadingStmt, rider_id, reading_lat, reading_lon, time);
            voltQueueSQL(log, rider_id, time, 2, "Loaded point (" + reading_lat + "," + reading_lon + ")into DB");
            Log.info("Loaded point in database for rider: " + rider_id);
            voltExecuteSQL(true);
        } catch (Exception e) {
            Log.info("Failed to Load point in database for rider: " + rider_id);
            throw new RuntimeException("Failed to load point:" + e);
        }

        // return successfull reading
        return BikerStreamConstants.BIKEREADING_SUCCESSFUL;
    }

}

