package edu.brown.benchmark.voterstream.procedures;

import org.voltdb.SQLStmt;
import org.voltdb.VoltTrigger;

public class StatisticVotesTrigger extends VoltTrigger {

    @Override
    protected String toSetStreamName() {
        return "votes_streamA";
    }
	
    // step 4: Insert vote (INSERT INTO votes)  
    public final SQLStmt insertVotesStmt = 
    	new SQLStmt("INSERT INTO votes (vote_id, phone_number, state, contestant_number, created) SELECT * FROM votes_streamA");
    
    // update total votes
    public final SQLStmt updateTotalVotesStmt = 
    	new SQLStmt("UPDATE total_votes SET num_votes = num_votes + 1 WHERE row_id = 1");

    // step 5: UPDATE votes_by_phone_number
    // why not just using S3 without T3? A: logically stream can not be updated
    public final SQLStmt insertT3 =
    	new SQLStmt("INSERT INTO T3 (phone_number, num_votes) SELECT votes_by_phone_number.* FROM votes_streamA, votes_by_phone_number WHERE votes_streamA.phone_number = votes_by_phone_number.phone_number;");

    public final SQLStmt updateT3 = 
    	new SQLStmt("UPDATE T3 SET num_votes = num_votes + 1");

    public final SQLStmt insertS3 =
    	new SQLStmt("INSERT INTO S3 (phone_number, num_votes) SELECT * from T3;");
    // above insert into stream statement will fire trigger (UpdateVoterByPhoneNumberTrigger) to update table votes_by_phone_number 

    public final SQLStmt deleteT3 = 
    	new SQLStmt("DELETE FROM T3");
  
    // FIXME, after using the tuple in stream, we should delete it. 
    public final SQLStmt deleteVotesStreamStmt = 
    	new SQLStmt("DELETE FROM votes_streamA");

}
