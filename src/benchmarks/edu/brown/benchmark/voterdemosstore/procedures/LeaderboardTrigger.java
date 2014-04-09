package edu.brown.benchmark.voterdemosstore.procedures;

import org.voltdb.ProcInfo;
import org.voltdb.SQLStmt;
import org.voltdb.StmtInfo;
import org.voltdb.VoltTrigger;

@ProcInfo (
	partitionInfo = "top_three_last_30_sec.contestant_number:1",
    singlePartition = true
)

public class LeaderboardTrigger extends VoltTrigger {

    @Override
    protected String toSetStreamName() {
        return "trending_leaderboard";
    }
    
    public final SQLStmt deleteLeaderboard = new SQLStmt(
            "DELETE FROM top_three_last_30_sec;"
    );

    public final SQLStmt updateLeaderboard = new SQLStmt(
            "INSERT INTO top_three_last_30_sec (contestant_number, num_votes) SELECT contestant_number, count(*) FROM trending_leaderboard GROUP BY contestant_number;"
    );

}
