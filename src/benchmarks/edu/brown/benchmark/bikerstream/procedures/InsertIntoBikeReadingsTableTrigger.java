package edu.brown.benchmark.bikerstream.procedures;
import org.voltdb.SQLStmt;
import org.voltdb.VoltTrigger;

public class InsertIntoBikeReadingsTableTrigger extends VoltTrigger {

    @Override
    protected String toSetStreamName() {
        return "bikereadings_stream";
    }

    // step 4: Records a vote
    public final SQLStmt insertVoteStmt = new SQLStmt(
        "INSERT INTO bikereadings (bike_id, reading_time, reading_lat, reading_lon) SELECT * FROM bikereadings_stream;"
    );
   
    // TODO - one next thing to do is to add a window...
    //public final SQLStmt insertIntoWin = new SQLStmt(
    //    "INSERT INTO W_ROWS (vote_id, phone_number, state, contestant_number, created) SELECT * FROM S1;"
    //);
}
