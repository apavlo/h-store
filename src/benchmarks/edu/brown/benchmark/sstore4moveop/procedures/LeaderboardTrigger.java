package edu.brown.benchmark.sstore4moveop.procedures;

import org.voltdb.ProcInfo;
import org.voltdb.SQLStmt;
import org.voltdb.StmtInfo;
import org.voltdb.VoltTrigger;

@ProcInfo (
//	partitionInfo = "treading_leaderboard.phone_number:0",
	partitionInfo = "s1.vote_id:0",
    singlePartition = true
)

public class LeaderboardTrigger extends VoltTrigger {

    @Override
    protected String toSetStreamName() {
//        return "trending_leaderboard";
        return "s1";
    }
    
}
