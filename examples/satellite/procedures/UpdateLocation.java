package procedures;

import org.voltdb.*;

@ProcInfo(
    partitionInfo = "LOCATION.ID: 0",
    singlePartition = true
)

/*
*
*/
public class UpdateLocation extends VoltProcedure {

    public final SQLStmt updateItem = new SQLStmt("UPDATE LOCATION " +
                    "SET LATITUDE=?, LONGITUDE=? " +
                    "WHERE ID=?;");

    public VoltTable[] run( long id, double latitude, double longitude) throws VoltAbortException {
        // Add a SQL statement to the execution queue. Queries
        // and DMLs may not be mixed in one batch.
        voltQueueSQL( updateItem, latitude, longitude, id);

        // Run all queued queries.
        VoltTable[] queryresults = voltExecuteSQL();
        return queryresults;
    }
}
