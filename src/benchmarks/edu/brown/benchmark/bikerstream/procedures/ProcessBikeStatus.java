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
import edu.brown.benchmark.bikerstream.BikerStreamUtil;
import org.apache.log4j.Logger;
import org.voltdb.SQLStmt;
import org.voltdb.VoltProcedure;
import org.voltdb.VoltTable;
import org.voltdb.types.TimestampType;

/**
 * This VoltProcedure will trigger on INSERT INTO bikeStatus STREAM and performs the following;
 *   a. Feed lastNBikeStatus WINDOW
 *   b. Pass the new data into s3 STREAM
 *   c. Calculate speed to feed to riderSpeeds STREAM
 *   b. Update the riderPositions TABLE  <-- need to be after Calculate speed
 */
public class ProcessBikeStatus extends VoltProcedure {
    private static final Logger LOG = Logger.getLogger(ProcessBikeStatus.class);

    protected void toSetTriggerTableName() {
        addTriggerTable("bikeStatus");
    }

    public final SQLStmt getUserCoordinate = new SQLStmt(
            "SELECT user_id, latitude, longitude, time FROM bikeStatus;"
    );

    public final SQLStmt getUserPreviousCoordinate = new SQLStmt(
            "SELECT TOP 1 user_id, latitude, longitude, time " +
                    "FROM riderPositions " +
                    "WHERE user_id = ?" +
                    "ORDER BY time DESC;"
    );

    public final SQLStmt removeRiderPositions = new SQLStmt(
            "DELETE FROM riderPositions WHERE user_id = ?;"
    );

    public final SQLStmt insertRiderPositions = new SQLStmt(
            "INSERT INTO riderPositions (user_id, latitude, longitude, time) " +
                    "SELECT              user_id, latitude, longitude, time FROM bikeStatus;"
    );

    public final SQLStmt feedLastNBikeStatusWindow = new SQLStmt(
            "INSERT INTO lastNBikeStatus (user_id, latitude, longitude, time) " +
                    "SELECT user_id, latitude, longitude, time FROM bikeStatus;"
    );

    public final SQLStmt feedS3Stream = new SQLStmt(
            "INSERT INTO s3 (user_id, latitude, longitude) " +
                    "SELECT  user_id, latitude, longitude FROM bikeStatus;"
    );

    public final SQLStmt feedRiderSpeedsStream = new SQLStmt(
            "INSERT INTO riderSpeeds (user_id, speed) VALUES (?, ?);"
    );

    public final SQLStmt removeUsedBikeStatusTuple = new SQLStmt(
            "DELETE FROM bikeStatus;"
    );

    public long run() {
        LOG.debug(" >>> Start running " + this.getClass().getSimpleName());


        //a. Feed lastNBikeStatus WINDOW
        voltQueueSQL(feedLastNBikeStatusWindow);
        voltExecuteSQL();

        //b. Pass the new data into s3 STREAM
        voltQueueSQL(feedS3Stream);
        voltExecuteSQL();

        //c. Calculate speed to feed to riderSpeeds STREAM
        voltQueueSQL(getUserCoordinate);
        VoltTable coordinate = voltExecuteSQL()[0];
        long user_id = coordinate.fetchRow(0).getLong("user_id");
        double x1 = coordinate.fetchRow(0).getDouble("latitude");
        double y1 = coordinate.fetchRow(0).getDouble("longitude");
        TimestampType t1 = coordinate.fetchRow(0).getTimestampAsTimestamp("time");

        voltQueueSQL(getUserPreviousCoordinate, user_id);
        VoltTable previousCoordinate = voltExecuteSQL()[0];
        if (previousCoordinate.getRowCount() > 0) {
            double x2 = previousCoordinate.fetchRow(0).getDouble("latitude");
            double y2 = previousCoordinate.fetchRow(0).getDouble("longitude");
            TimestampType t2 = previousCoordinate.fetchRow(0).getTimestampAsTimestamp("time");


            final long MILLISECOND_TO_HOUR = 60 * 60 * 1000;
            double timeDiff = Math.abs(t1.getMSTime() - t2.getMSTime()) ;
            if (timeDiff > 0) {
                double distance = BikerStreamUtil.geoDistance(x1, y1, x2, y2);
                double speed = BikerStreamConstants.SPEED_SCALING * (distance / (timeDiff / MILLISECOND_TO_HOUR));

                /*
                LOG.info(x1 + ", " + y1 + ", " + x2 + ", " + y2);
                LOG.info(t1.toString());
                LOG.info(t2.toString());
                LOG.info(" CALCULATED distance: " + distance + "(miles)");
                LOG.info(" CALCULATED time    : " + (timeDiff / MILLISECOND_TO_HOUR)  + "(hours)");
                LOG.info(" CALCULATED speed   : " + speed + "(MPH)");
                */

                voltQueueSQL(feedRiderSpeedsStream, user_id, speed);
                voltExecuteSQL();
            }
        }

        //b. Update the riderPositions TABLE  <-- need to be after Calculate speed
        voltQueueSQL(removeRiderPositions, user_id);
        voltExecuteSQL();
        voltQueueSQL(insertRiderPositions);
        voltExecuteSQL();

        voltQueueSQL(removeUsedBikeStatusTuple);
        voltExecuteSQL(true);

        LOG.info(" <<< Finished running " + this.getClass().getSimpleName());
        return 0;
    }
}
