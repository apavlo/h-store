package edu.brown.benchmark.voterstream.procedures;

import org.voltdb.SQLStmt;
import org.voltdb.VoltTrigger;

public class UpdateVoterByPhoneNumberTrigger extends VoltTrigger {

    @Override
    protected String toSetStreamName() {
        return "S3";
    }
	
    // step 5 (continued): UPDATE votes_by_phone_number  
    public final SQLStmt insertVotesByPhoneNumber = 
    	new SQLStmt("INSERT INTO votes_by_phone_number (phone_number, num_votes) SELECT * FROM S3");
    
    // FIXME, after using the tuple in stream, we should delete it. 
    public final SQLStmt deleteS3 = 
    	new SQLStmt("DELETE FROM S3");

}
