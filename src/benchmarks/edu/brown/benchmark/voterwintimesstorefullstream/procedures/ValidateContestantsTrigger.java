package edu.brown.benchmark.voterwintimesstorefullstream.procedures;
import org.voltdb.SQLStmt;
import org.voltdb.VoltTrigger;

public class ValidateContestantsTrigger extends VoltTrigger {

    @Override
    protected String toSetStreamName() {
        return "S2";
    }

     // step 1: Validate contestants
    public final SQLStmt insertS1Stmt = 
        new SQLStmt("INSERT INTO S3 (vote_id, phone_number, state, contestant_number, time) SELECT S2.vote_id, S2.phone_number, S2.state, S2.contestant_number, S2.time FROM S2, contestants WHERE S2.contestant_number=contestants.contestant_number;");
    
    // FIXME, after using the tuple in stream, we should delete it. 
//    public final SQLStmt deleteVotesStreamStmt = 
//      new SQLStmt("DELETE FROM votes_stream");

}
