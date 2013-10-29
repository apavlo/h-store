package edu.brown.benchmark.anotherstream.procedures;

import org.voltdb.SQLStmt;
import org.voltdb.StmtInfo;
import org.voltdb.VoltTrigger;

public class UpdateVotesAndTotalVotesTrigger extends VoltTrigger {

    @Override
    protected String toSetStreamName() {
        return "S2";
    }

    // step 4: Records a vote
    public final SQLStmt insertVoteStmt = new SQLStmt(
        "INSERT INTO votes (vote_id, phone_number, state, contestant_number, created) SELECT * FROM S2;"
    );
    
    // step 5: update number of votes
    // here we hack insert behavior to do update thing (UPSERT) 
    @StmtInfo(
            upsertable=true
        )
    public final SQLStmt insertVotesByPhoneNumberStmt = 
        new SQLStmt("INSERT INTO votes_by_phone_number ( phone_number, num_votes ) SELECT votes_by_phone_number.phone_number, votes_by_phone_number.num_votes + 1 FROM votes_by_phone_number, S2 WHERE votes_by_phone_number.phone_number=S2.phone_number;");

    
    // step 6: update VotesByContestantNumberState
    @StmtInfo(
            upsertable=true
        )
    public final SQLStmt insertVotesByContestantNumberStateStmt = 
        new SQLStmt("INSERT INTO votes_by_contestant_number_state ( contestant_number, state, num_votes ) SELECT votes_by_contestant_number_state.contestant_number, votes_by_contestant_number_state.state, votes_by_contestant_number_state.num_votes+1 FROM votes_by_contestant_number_state, S2 WHERE (votes_by_contestant_number_state.contestant_number=S2.contestant_number) and (votes_by_contestant_number_state.state=S2.state);");

    
    // FIXME, after using the tuple in stream, we should delete it. 
//    public final SQLStmt deleteStreamStmt = 
//        new SQLStmt("DELETE FROM S2");

}
