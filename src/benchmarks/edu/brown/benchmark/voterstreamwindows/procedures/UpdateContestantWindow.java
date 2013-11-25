package edu.brown.benchmark.voterstreamwindows.procedures;

import org.voltdb.SQLStmt;
import org.voltdb.VoltTrigger;

public class UpdateContestantWindow extends VoltTrigger {

    @Override
    protected String toSetStreamName() {
        return "S5";
    }

    
 // step 7-1: insert temp stream S4
    public final SQLStmt insertWindowStmt = new SQLStmt(
            "INSERT INTO W_ROWS (vote_id, contestant_number, created) SELECT vote_id, contestant_number, created FROM S5;"
        );
    
    // FIXME, after using the tuple in stream, we should delete it. 
    public final SQLStmt deleteStreamStmt = 
        new SQLStmt("DELETE FROM S5");

}
