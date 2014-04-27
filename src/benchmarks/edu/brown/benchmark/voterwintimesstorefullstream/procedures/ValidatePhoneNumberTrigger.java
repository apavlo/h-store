package edu.brown.benchmark.voterwintimesstorefullstream.procedures;
import org.voltdb.SQLStmt;
import org.voltdb.VoltTrigger;

public class ValidatePhoneNumberTrigger extends VoltTrigger {

    @Override
    protected String toSetStreamName() {
        return "votes_stream";
    }

     // step 1: Validate contestants
    public final SQLStmt insertS1Stmt = 
        new SQLStmt("INSERT INTO S1 (vote_id, phone_number, area_code, state, fake_state, contestant_number, time) SELECT votes_stream.* FROM votes_stream, v_votes_by_phone_number WHERE votes_stream.phone_number=v_votes_by_phone_number.phone_number AND num_votes < 10;");
    
    // FIXME, after using the tuple in stream, we should delete it. 
//    public final SQLStmt deleteVotesStreamStmt = 
//      new SQLStmt("DELETE FROM votes_stream");

}
