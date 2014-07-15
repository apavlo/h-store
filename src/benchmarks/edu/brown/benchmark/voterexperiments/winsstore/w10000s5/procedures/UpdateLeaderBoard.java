package edu.brown.benchmark.voterexperiments.winsstore.w10000s5.procedures;

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
        "INSERT INTO leaderboard (contestant_number, numvotes, phone_number) SELECT contestant_number, count(*), max(phone_number) FROM W_ROWS GROUP BY contestant_number;"
    );
}
