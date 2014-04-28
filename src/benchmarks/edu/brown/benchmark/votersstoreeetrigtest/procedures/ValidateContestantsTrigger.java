package edu.brown.benchmark.votersstoreeetrigtest.procedures;
import org.voltdb.SQLStmt;
import org.voltdb.VoltTrigger;

public class ValidateContestantsTrigger extends VoltTrigger {

    @Override
    protected String toSetStreamName() {
        return "S1";
    }

     // step 1: Validate contestants
    public final SQLStmt insertS1Stmt = 
        new SQLStmt("INSERT INTO votes (vote_id, phone_number, state, contestant_number, ts) SELECT S1.* FROM S1, contestants WHERE S1.contestant_number=contestants.contestant_number;");
    
    // FIXME, after using the tuple in stream, we should delete it. 
//    public final SQLStmt deleteVotesStreamStmt = 
//      new SQLStmt("DELETE FROM votes_stream");

}
