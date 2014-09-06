package edu.brown.benchmark.bikerstream.procedures;

import org.voltdb.SQLStmt;
import org.voltdb.VoltProcedure;
import org.voltdb.VoltTable;

/**
 * Query the content of recentRiderArea table.
 */
public class GetRecentRiderArea extends VoltProcedure {

    public final SQLStmt getRecentRiderArea = new SQLStmt(
            "SELECT latitude_1, longitude_1, latitude_2, longitude_2, sqr_mile FROM recentRiderArea;"
    );

    public VoltTable run() {
        voltQueueSQL(getRecentRiderArea);
        return voltExecuteSQL(true)[0];
    }
}
