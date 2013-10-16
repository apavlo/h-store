package edu.brown.benchmark.anothervoterstream.procedures;

import org.voltdb.SQLStmt;
import org.voltdb.StmtInfo;
import org.voltdb.VoltTrigger;

import edu.brown.hstore.conf.ConfigProperty;

public class UpdateVotesByPhoneNumberTrigger extends VoltTrigger {

    @Override
    protected String toSetStreamName() {
        return "S3";
    }

    // step 5: Validate number of votes
    // FIXME: here we hack insert behavior to do update thing 
    @StmtInfo(
            upsertable=true
        )
    public final SQLStmt insertVotesByPhoneNumberStmt = 
        new SQLStmt("INSERT INTO votes_by_phone_number ( phone_number, num_votes ) SELECT phone_number, num_votes + 1 FROM S3;");
    
    // FIXME, after using the tuple in stream, we should delete it. 
    public final SQLStmt deleteStreamStmt = new SQLStmt("DELETE FROM S3");


}
