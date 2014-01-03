package edu.brown.benchmark.voterwinsstore.procedures;

import org.voltdb.SQLStmt;
import org.voltdb.VoltTrigger;

public class UpdateLeaderBoard extends VoltTrigger{
	@Override
    protected String toSetStreamName() {
        return "W_ROWS";
    }

    // step 4: Records a vote
    public final SQLStmt deleteLeaderBoard = new SQLStmt(
        "DELETE FROM leaderboard;"
    );
    
    public final SQLStmt insertIntoLeaderBoard = new SQLStmt(
        "INSERT INTO leaderboard (contestant_number, numvotes) SELECT contestant_number, count(*) FROM W_ROWS GROUP BY contestant_number;"
    );
}
