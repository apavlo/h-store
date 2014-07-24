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
    
    public final SQLStmt getUser = new SQLStmt(
    	"SELECT * FROM users WHERE user_id = ?"
    );
    
    public final SQLStmt getBike = new SQLStmt(
    	"SELECT * FROM bikes WHERE user_id = ?"
    );

    // Enters a bike ride gps event
    public final SQLStmt log = new SQLStmt(
        "INSERT INTO logs (user_id, time, success, action) " +
        "VALUES (?, ?, ?, ?);"
    );

    public long run(long rider_id, double reading_lat, double reading_lon) throws Exception {

        try {
            // Post the ride event
        	voltQueueSQL(getUser, rider_id);
        	voltQueueSQL(getBike, rider_id);
        	VoltTable results[] = voltExecuteSQL();
        	if (results[0].getRowCount() < 1)
        		return BikerStreamConstants.USER_DOESNT_EXIST;
        		//throw new RuntimeException("Rider: " + rider_id + " does not exist");
        	if (results[1].getRowCount() < 1)
        		return BikerStreamConstants.NO_BIKE_CHECKED_OUT;
        		//throw new RuntimeException("Rider: " + rider_id + " does not have a bike checked out");
            TimestampType time = new TimestampType();
            voltQueueSQL(insertBikeReadingStmt, rider_id, reading_lat, reading_lon, time);
            voltExecuteSQL(true);
        } catch (Exception e) {
            return BikerStreamConstants.FAILED_POINT_ADD;
        }

        // return sucessfull reading
        return BikerStreamConstants.BIKEREADING_SUCCESSFUL;
    }

}

