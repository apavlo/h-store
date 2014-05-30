package edu.brown.benchmark.bikerstream.procedures;

import org.voltdb.SQLStmt;
import org.voltdb.VoltTrigger;


public class UpdateCount extends VoltTrigger{

    @Override
    protected String toSetStreamName() {
        return "bikereadings_window_rows";
    }

    /**
     * Window Trigger event.
     * For each slide in the window, take a count of all currently held points in the window
     * for each Bike_ID and store it into table Count_bikereadings_table along with the most
     * recent timestamp for that bike.
     */

    public final SQLStmt InsertIntoCountTable = new SQLStmt(
            "INSERT INTO count_bikereadings_table (bike_id, count_time, count_val) " +
            "SELECT bike_id, max(reading_time), count(*) FROM bikereadings_window_rows GROUP BY bike_id;");
}
