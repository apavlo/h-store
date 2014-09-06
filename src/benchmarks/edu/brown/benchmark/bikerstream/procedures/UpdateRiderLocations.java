package edu.brown.benchmark.bikerstream.procedures;
import org.voltdb.SQLStmt;
import org.voltdb.StmtInfo;
import org.voltdb.VoltProcedure;
import org.voltdb.VoltTable;
import org.voltdb.types.TimestampType;

/* trigger to insert from bikereadings stream to
 * bikereadings table */

public class UpdateRiderLocations extends VoltProcedure {

    @Override
    protected void toSetTriggerTableName() {
        addTriggerTable("bikeStatus");
    }

    @StmtInfo(upsertable=true)
    public final SQLStmt updateBikeStatus = new SQLStmt(
            "INSERT INTO userLocations SELECT user_id, latitude, longitude FROM bikeStatus"
    );

    public long run() {
        voltQueueSQL(updateBikeStatus);
        voltExecuteSQL(true);
        return 0;
    }
}


