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
import org.jfree.util.Log;
import org.voltdb.ProcInfo;
import org.voltdb.SQLStmt;
import org.voltdb.VoltProcedure;
import org.voltdb.VoltTable;
import org.voltdb.types.TimestampType;

@ProcInfo (
    singlePartition = true
)
public class AcceptDiscount extends VoltProcedure {

    // Logging Information
    private static final Logger Log = Logger.getLogger(AcceptDiscount.class);

    public final SQLStmt getStation = new SQLStmt(
                "SELECT * FROM stationstatus where station_id = ?"
            );

    public final SQLStmt updateStation = new SQLStmt(
                "UPDATE stationstatus SET current_discount = ? where station_id = ?"
            );

    public final SQLStmt addDiscount = new SQLStmt(
                "INSERT INTO discounts (user_id, station_id) VALUES (?,?)"
            );

    public final SQLStmt logSuccess = new SQLStmt(
                "INSERT INTO logs (user_id, time, success, action) VALUES (?,?,1,?)"
            );


    public long run(long rider_id, long station_id) throws Exception {

        voltQueueSQL(getStation, station_id);
        VoltTable results[] = voltExecuteSQL();
        assert(results[0].getRowCount() == 1);

        long numDiscs = results[0].fetchRow(0).getLong("current_discount");

        if (numDiscs > 0) {
            Log.info("Discount claimed from station: " + station_id + " by rider: " + rider_id);
            voltQueueSQL(updateStation, numDiscs - 1, station_id);
            voltQueueSQL(addDiscount, rider_id, station_id);
            voltQueueSQL(logSuccess, rider_id, new TimestampType(), "Got discount for station: " + station_id);
            voltExecuteSQL(true);
            return station_id;
        } else {
            throw new RuntimeException("There are no discounts available at station: " + station_id);
        }

    }

} // End Class

