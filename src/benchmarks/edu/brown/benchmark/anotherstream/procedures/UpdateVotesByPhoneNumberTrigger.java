package edu.brown.benchmark.anotherstream.procedures;

import org.voltdb.SQLStmt;
import org.voltdb.VoltTrigger;

public class UpdateVotesByPhoneNumberTrigger extends VoltTrigger {

    @Override
    protected String toSetStreamName() {
        return "S3";
    }

    // step 5: Validate number of votes
    // FIXME: here we hack insert behavior to do update thing 
    public final SQLStmt insertVotesByPhoneNumberStmt = 
        new SQLStmt("INSERT INTO votes_by_phone_number ( phone_number, num_votes ) SELECT * FROM S3;");
    
    // delete the content of T3
    public final SQLStmt deleteT3Stmt = new SQLStmt("DELETE FROM T3;");
    
    // FIXME, after using the tuple in stream, we should delete it. 
    public final SQLStmt deleteStreamStmt = new SQLStmt("DELETE FROM S3");

}
