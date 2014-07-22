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
import org.voltdb.*;


/**
 * This VoltProcedure will trigger on INSERT INTO s1 STREAM and performs the following;
 *   a.  Replaces the near by stations that offer discount for a given user in the nearByDiscounts TABLE
 */
public class S1Trigger extends VoltProcedure {
    private static final Logger LOG = Logger.getLogger(S1Trigger.class);

    protected void toSetTriggerTableName() {
        addTriggerTable("s1");
    }

    public final SQLStmt getUserFromS1 = new SQLStmt(
            "SELECT user_id FROM s1;"
    );

    public final SQLStmt getNearByDiscounts = new SQLStmt(
            "SELECT nbs.user_id, nbs.station_id " +
                    "FROM nearByStations AS nbs, StationStatus AS ss " +
                    "WHERE nbs.station_id = ss.station_id " +
                    "    AND ss.current_discount > 0 " +
                    "    AND user_id = ?;"
    );

    public final SQLStmt insertNearByDiscounts = new SQLStmt(
            "INSERT INTO nearByDiscounts (user_id, station_id) VALUES (?, ?);"
    );

    public final SQLStmt clearUserNearByDiscounts = new SQLStmt(
            "DELETE FROM NearByDiscounts WHERE user_id = ?;"
    );

    public final SQLStmt getNearByDiscountsByUser = new SQLStmt(
            "SELECT * FROM NearByDiscounts WHERE user_id = ?;"
    );

    public final SQLStmt removeUsedS1Tuple = new SQLStmt("" +
            "DELETE FROM S1;"
    );

    public long run() {
        LOG.debug(" >>> Start running " + this.getClass().getSimpleName());
        // Get a handle on the new tuple
        voltQueueSQL(getUserFromS1);
        VoltTable coordinates[] = voltExecuteSQL();
        long user_id = coordinates[0].fetchRow(0).getLong("user_id");

        // Clear existing data based on the new tuple
        voltQueueSQL(getNearByDiscounts, user_id);
        voltQueueSQL(clearUserNearByDiscounts, user_id);
        VoltTable tempResults[] = voltExecuteSQL();

        for (int i = 0; i < tempResults[0].getRowCount(); i++) {
            voltQueueSQL(insertNearByDiscounts, user_id, tempResults[0].fetchRow(i).getLong("station_id"));
            voltExecuteSQL();
        }

        /*
        // For verification purpose
        voltQueueSQL(getNearByDiscountsByUser, user_id);
        LOG.info("Replaced near by discount for user_id " + user_id + ": " + voltExecuteSQL()[0]);
        */

        voltQueueSQL(removeUsedS1Tuple);
        voltExecuteSQL(true);

        LOG.info(" <<< Finished running " + this.getClass().getSimpleName() + " for rider: " + user_id);
        return 0;
    }
}

