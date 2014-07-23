package edu.brown.benchmark.voterexperiments.demosstorecorrect.procedures;

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
        return "trending_leaderboard";
    }
    
    public final SQLStmt deleteLeaderboard = new SQLStmt(
            "DELETE FROM leaderboard;"
    );

    public final SQLStmt updateLeaderboard = new SQLStmt(
            "INSERT INTO leaderboard (contestant_number, num_votes) SELECT trending_leaderboard.contestant_number, count(*) FROM trending_leaderboard, contestants WHERE trending_leaderboard.contestant_number = contestants.contestant_number GROUP BY trending_leaderboard.contestant_number;"
    );

}
