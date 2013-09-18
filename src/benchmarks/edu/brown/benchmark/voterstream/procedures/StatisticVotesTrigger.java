package edu.brown.benchmark.voterstream.procedures;

import org.voltdb.SQLStmt;
import org.voltdb.VoltTrigger;

public class StatisticVotesTrigger extends VoltTrigger {

	@Override
	protected String toSetStreamName() {
		return "votes_streamA";
	}
	
    public final SQLStmt insertVotesStmt = 
    	new SQLStmt("INSERT INTO votes (vote_id, phone_number, state, contestant_number, created) SELECT * FROM votes_streamA");
    
    public final SQLStmt updateTotalVotesStmt = 
    	new SQLStmt("UPDATE total_votes SET num_votes = num_votes + 1 WHERE row_id = 1");
    
    public final SQLStmt deleteVotesStreamStmt = 
    	new SQLStmt("DELETE FROM votes_streamA");

}
