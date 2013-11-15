package edu.brown.benchmark.voterstreamwindows.procedures;

import org.voltdb.SQLStmt;
import org.voltdb.VoltTrigger;

public class UpdateLeaderBoard extends VoltTrigger {

    @Override
    protected String toSetStreamName() {
        return "S6";
    }
    
    public final SQLStmt insertAggregate = new SQLStmt(
            "INSERT INTO current_leader (contestant_number, created, numVotes) SELECT * FROM S6 ORDER BY numVotes DESC LIMIT 1;"
        );
    
    // FIXME, after using the tuple in stream, we should delete it. 
    public final SQLStmt deleteStreamStmt = 
        new SQLStmt("DELETE FROM S6");

}
