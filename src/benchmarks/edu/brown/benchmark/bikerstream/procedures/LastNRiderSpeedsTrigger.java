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

import org.apache.log4j.Logger;
import org.voltdb.SQLStmt;
import org.voltdb.VoltProcedure;
import org.voltdb.VoltTable;


/**
 * TODO:  This should extend VoltTrigger instead of VoltProcedure
 * as its trigger source is a WINDOW.
 * When extends VoltTrigger, this trigger will not get schedule.
 * As a work around, it extends VoltProcedure instead.
 *
 * This VoltProcedure will trigger on INSERT INTO lastNRiderSpeeds WINDOW and
 * Aggregate the result into recentRiderSummary TABLE
 */
public class LastNRiderSpeedsTrigger extends VoltProcedure {
    private static final Logger LOG = Logger.getLogger(LastNRiderSpeedsTrigger.class);

    protected void toSetTriggerTableName() {
        addTriggerTable("lastNRiderSpeeds");
    }

    public final SQLStmt deleteRecentRiderSummary = new SQLStmt(
            "DELETE FROM recentRiderSummary;"
    );

    public final SQLStmt getNewRecentRiderSummary = new SQLStmt(
            "SELECT COUNT(DISTINCT user_id) AS rider_count, MAX(speed) AS speed_max, MIN(speed) AS speed_min, AVG(speed) AS speed_avg FROM lastNRiderSpeeds;"
    );

    public final SQLStmt insertRecentRiderSummary = new SQLStmt(
            "INSERT INTO recentRiderSummary (rider_count, speed_max, speed_min, speed_avg) VALUES (?, ?, ?, ?);"
    );

    public long run() {
        LOG.info(" >>> Start running " + this.getClass().getSimpleName());

        voltQueueSQL(getNewRecentRiderSummary);
        VoltTable newSummary = voltExecuteSQL()[0];
        long riderCount = 0;
        double speedMax = 0.0;
        double speedMin = 0.0;
        double speedAvg = 0.0;
        if (newSummary.getRowCount() > 0) {
            riderCount = newSummary.fetchRow(0).getLong("rider_count");
            speedMax = newSummary.fetchRow(0).getDouble("speed_max");
            speedMin = newSummary.fetchRow(0).getDouble("speed_min");
            speedAvg = newSummary.fetchRow(0).getDouble("speed_avg");
        }

        voltQueueSQL(deleteRecentRiderSummary);
        voltQueueSQL(insertRecentRiderSummary, riderCount, speedMax, speedMin, speedAvg);
        voltExecuteSQL(true);

        LOG.info(" <<< Finished running " + this.getClass().getSimpleName());
        return 0;
    }
}

