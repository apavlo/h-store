package edu.brown.benchmark.anotherstream.procedures;

import org.voltdb.SQLStmt;
import org.voltdb.VoltTrigger;

public class ValidateContestantsTrigger extends VoltTrigger {

    @Override
    protected String toSetStreamName() {
        return "votes_stream";
    }

     // step 1: Validate contestants
    public final SQLStmt insertS1Stmt = 
    	new SQLStmt("INSERT INTO S1 (vote_id, phone_number, state, contestant_number) SELECT votes_stream.* FROM votes_stream, contestants WHERE votes_stream.contestant_number=contestants.contestant_number;");
	
    // FIXME, after using the tuple in stream, we should delete it. 
    public final SQLStmt deleteVotesStreamStmt = 
    	new SQLStmt("DELETE FROM votes_stream");

}
