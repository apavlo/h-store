package edu.brown.benchmark.voterexperiments.winsstore.w1000s100.procedures;
import org.voltdb.SQLStmt;
import org.voltdb.VoltTrigger;

public class UpdateVotesAndTotalVotesTrigger extends VoltTrigger {

    @Override
    protected String toSetStreamName() {
        return "S1";
    }

    // step 4: Records a vote
    public final SQLStmt insertVoteStmt = new SQLStmt(
        "INSERT INTO votes (vote_id, phone_number, state, contestant_number, created) SELECT * FROM S1;"
    );
    
    public final SQLStmt insertIntoWin = new SQLStmt(
        "INSERT INTO W_ROWS (vote_id, phone_number, state, contestant_number, created) SELECT * FROM S1;"
    );
}
