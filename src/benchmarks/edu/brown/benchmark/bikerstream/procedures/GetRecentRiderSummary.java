package edu.brown.benchmark.bikerstream.procedures;

import org.voltdb.SQLStmt;
import org.voltdb.VoltProcedure;
import org.voltdb.VoltTable;

/**
 * Query the content of recentRiderSummary table.
 */
public class GetRecentRiderSummary extends VoltProcedure {

    public final SQLStmt getRecentRiderSummary = new SQLStmt(
            "SELECT rider_count, speed_max, speed_min , speed_avg FROM recentRiderSummary;"
    );

    public VoltTable run() {
        voltQueueSQL(getRecentRiderSummary);
        return voltExecuteSQL(true)[0];
    }
}
