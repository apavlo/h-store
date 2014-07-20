package edu.brown.benchmark.bikerstream.procedures;
import org.voltdb.SQLStmt;
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

    // Insert from stream to window
    public final SQLStmt getId = new SQLStmt(
            "SELECT user_id FROM bikeStatus"
    );

    public final SQLStmt deleteOld = new SQLStmt(
            "DELETE FROM userLocations WHERE user_id = ?"
    );

    // Insert from stream to table
    public final SQLStmt insertlocations = new SQLStmt(
            "INSERT INTO userLocations (SELECT user_id, latitude, longitude FROM bikeStatus)"
    );

    public final SQLStmt deleteBikeStatus = new SQLStmt(
            "DELETE FROM bikeStatus"
    );


    public long run() {


        voltQueueSQL(getId);
        VoltTable results = voltExecuteSQL()[0];

        int rowCount = results.getRowCount();
        for (int i = 0; i < rowCount; ++i){
            voltQueueSQL(deleteOld, results.fetchRow(i).getLong(0));
        }

        voltQueueSQL(insertlocations);
        voltQueueSQL(deleteBikeStatus);
        voltExecuteSQL(true);

        return 0;
    }


}


