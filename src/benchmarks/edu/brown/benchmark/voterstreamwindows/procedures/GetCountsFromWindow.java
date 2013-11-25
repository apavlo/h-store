package edu.brown.benchmark.voterstreamwindows.procedures;

import org.voltdb.SQLStmt;
import org.voltdb.VoltTrigger;

public class GetCountsFromWindow extends VoltTrigger {

    @Override
    protected String toSetStreamName() {
        return "W_ROWS";
    }
    
    public final SQLStmt insertAggregate = new SQLStmt(
            "INSERT INTO S6 (contestant_number, created, numVotes) SELECT contestant_number, MIN(created), COUNT(*) AS CNT FROM W_ROWS GROUP BY contestant_number;"
        );

}
