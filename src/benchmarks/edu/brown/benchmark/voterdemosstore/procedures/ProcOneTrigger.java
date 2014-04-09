package edu.brown.benchmark.voterdemosstore.procedures;

import org.voltdb.SQLStmt;
import org.voltdb.StmtInfo;
import org.voltdb.VoltTrigger;

public class ProcOneTrigger extends VoltTrigger {

    @Override
    protected String toSetStreamName() {
        return "votes_stream";
    }

    public final SQLStmt insertVote = new SQLStmt(
            "INSERT INTO votes (vote_id, phone_number, state, contestant_number, time) SELECT * FROM votes_stream;"
    );
    
    public final SQLStmt procOut = new SQLStmt(
            "INSERT INTO proc_one_out (vote_id, phone_number, state, contestant_number, time) SELECT * FROM votes_stream;"
    );
    
}
