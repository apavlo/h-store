package edu.brown.benchmark.bikerstream.procedures;

import org.voltdb.SQLStmt;
import org.voltdb.VoltTrigger;

public class UpdateCount extends VoltTrigger{
	@Override
    protected String toSetStreamName() {
        return "bikereadings_window_rows";
    }

    // leaderboard app deletes state from the leaderboard
    // table - I don't want to delete in this app (yet) 
    //public final SQLStmt deleteLeaderBoard = new SQLStmt(
    //    "DELETE FROM leaderboard;"
    //);
    
    public final SQLStmt InsertIntoCountTable = new SQLStmt(
        "INSERT INTO count_bikereadings_table (bike_id, count_time, count) SELECT bike_id, max(reading_time), count(*) FROM bikereadings_window_rows GROUP BY bike_id;");
}
