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
import org.apache.log4j.Logger;
import org.voltdb.SQLStmt;
import org.voltdb.VoltProcedure;
import org.voltdb.VoltTable;

/**
 * This VoltProcedure will trigger on INSERT INTO riderSpeeds STREAM and check the speed limit
 *   a. Feed lastNRiderSpeeds WINDOW
 *   b. Detect Anomalies
 */
public class DetectAnomalies extends VoltProcedure {
    private static final Logger LOG = Logger.getLogger(DetectAnomalies.class);

    protected void toSetTriggerTableName() {
        addTriggerTable("riderSpeeds");
    }

    public final SQLStmt feedLastNRiderSpeedsWindow = new SQLStmt(
            "INSERT INTO lastNRiderSpeeds (user_id, speed) SELECT user_id, speed FROM riderSpeeds;"
    );

    public final SQLStmt getOverSpeed = new SQLStmt(
            "SELECT user_id, speed FROM riderSpeeds WHERE speed >= " + BikerStreamConstants.STOLEN_SPEED + ";"
    );

    public final SQLStmt removeOldAnomalies = new SQLStmt(
            "DELETE FROM anomalies WHERE user_id = ?;"
    );

    public final SQLStmt insertAnomalies = new SQLStmt(
            "INSERT INTO anomalies (user_id, status) VALUES (?, ?);"
    );

    public final SQLStmt removeUsedRiderSpeedsTuple = new SQLStmt(
            "DELETE FROM riderSpeeds;"
    );

    // Only for debug
    public final SQLStmt checkAnomalies = new SQLStmt(
            "SELECT user_id, COUNT(*) FROM anomalies GROUP BY user_id ORDER BY user_id;"
    );

    public long run() {
        LOG.debug(" >>> Start running " + this.getClass().getSimpleName());
        // a. Feed lastNRiderSpeeds WINDOW
        voltQueueSQL(feedLastNRiderSpeedsWindow);
        voltExecuteSQL();

        // b. Detect Anomalies
        voltQueueSQL(getOverSpeed);
        VoltTable anomaly = voltExecuteSQL()[0];

        long user_id;
        double speed;
        for (int i = 0; i < anomaly.getRowCount(); i++) {
            user_id = anomaly.fetchRow(i).getLong("user_id");
            speed = anomaly.fetchRow(i).getDouble("speed");

            LOG.info("   Anomaly detected rider: " + user_id + " is speeding at " + speed + " MPH!");

            voltQueueSQL(removeOldAnomalies, user_id);
            voltExecuteSQL();
            voltQueueSQL(insertAnomalies, user_id, BikerStreamConstants.STOLEN_STATUS);
            voltExecuteSQL();
        }

        /*
        // For verification purpose
        voltQueueSQL(checkAnomalies);
        LOG.info("Summary of anomalies: " + voltExecuteSQL()[0]);
        */

        voltQueueSQL(removeUsedRiderSpeedsTuple);
        voltExecuteSQL(true);

        LOG.info(" <<< Finished running " + this.getClass().getSimpleName());
        return 0;
    }
}
