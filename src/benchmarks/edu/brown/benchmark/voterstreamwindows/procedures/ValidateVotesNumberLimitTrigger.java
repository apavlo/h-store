package edu.brown.benchmark.voterstreamwindows.procedures;

import org.voltdb.SQLStmt;
import org.voltdb.VoltTrigger;

public class ValidateVotesNumberLimitTrigger extends VoltTrigger {

    @Override
    protected String toSetStreamName() {
        return "S1";
    }

    // step 2: Validate number of votes
    // FIXME: here the threshold is hardcode, later we should get it from some parameter table. 
    public final SQLStmt insertS1Stmt = 
        new SQLStmt("INSERT INTO S2 (vote_id, phone_number, state, contestant_number, created) SELECT S1.* FROM S1, votes_by_phone_number WHERE (S1.phone_number=votes_by_phone_number.phone_number) and (votes_by_phone_number.num_votes<10);");
    
    // FIXME, after using the tuple in stream, we should delete it. 
    //public final SQLStmt deleteStreamStmt = 
    //    new SQLStmt("DELETE FROM S1");

}
