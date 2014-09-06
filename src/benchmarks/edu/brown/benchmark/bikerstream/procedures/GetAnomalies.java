package edu.brown.benchmark.bikerstream.procedures;

import org.voltdb.SQLStmt;
import org.voltdb.VoltProcedure;
import org.voltdb.VoltTable;

/**
 * Query the content of anomalies table.
 */
public class GetAnomalies extends VoltProcedure {

    public final SQLStmt getAnomalies = new SQLStmt(
            "SELECT user_id, status FROM anomalies ORDER BY user_id;"
    );

    public VoltTable run() {
        voltQueueSQL(getAnomalies);
        return voltExecuteSQL(true)[0];
    }
}
