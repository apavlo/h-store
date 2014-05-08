package edu.brown.benchmark.microwinsstore.procedures;

import org.voltdb.ProcInfo;
import org.voltdb.SQLStmt;
import org.voltdb.StmtInfo;
import org.voltdb.VoltTrigger;

@ProcInfo (
	partitionInfo = "leaderboard.contestant_number:1",
    singlePartition = true
)

public class LeaderboardTrigger extends VoltTrigger {

    @Override
    protected String toSetStreamName() {
        return "w_rows";
    }
    
    public final SQLStmt deleteLeaderboard = new SQLStmt(
            "DELETE FROM leaderboard;"
    );

    public final SQLStmt updateLeaderboard = new SQLStmt(
            "INSERT INTO leaderboard (contestant_number, numvotes) SELECT contestant_number, count(*) FROM w_rows GROUP BY contestant_number;"
    );

}
