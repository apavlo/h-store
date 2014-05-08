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

import org.voltdb.ProcInfo;
import org.voltdb.SQLStmt;
import org.voltdb.VoltProcedure;
import org.voltdb.VoltTable;
import org.voltdb.types.TimestampType;

import edu.brown.benchmark.bikerstream.BikerStreamConstants;

@ProcInfo (
    // TODO - don't know what to do about partitioninng
    //partitionInfo = "votes.phone_number:1",
    //singlePartition = true
)
public class RideBike extends VoltProcedure {
	
    // Enters a bike ride gps event 
    public final SQLStmt insertBikeReadingStmt = new SQLStmt(
		"INSERT INTO bikereadings_stream (bike_id, reading_time, reading_lat, reading_lon) VALUES (?, ?, ?, ?);"
    );
	
    public long run(int bikeId, long reading_lat, long reading_lon) {
		
        // Post the ride event 
        TimestampType timestamp = new TimestampType();
        voltQueueSQL(insertBikeReadingStmt, bikeId, timestamp, reading_lat, reading_lon);
        voltExecuteSQL(true);
		
        // Set the return value to 0: successful vote
        return BikerStreamConstants.BIKEREADING_SUCCESSFUL;
    }

}

