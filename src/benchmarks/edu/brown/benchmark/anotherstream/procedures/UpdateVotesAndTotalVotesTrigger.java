package edu.brown.benchmark.anotherstream.procedures;

import org.voltdb.SQLStmt;
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
    
    // update total number of validate votes 
    public final SQLStmt updateTotalVotesStmt = 
            new SQLStmt("UPDATE total_votes SET num_votes = num_votes + 1");
   
    
    // step 5-1: insert temp table T3
    public final SQLStmt insertT3Stmt = new SQLStmt(
            "INSERT INTO T3 (phone_number, num_votes) SELECT votes_by_phone_number.* FROM votes_by_phone_number, S2 WHERE votes_by_phone_number.phone_number=S2.phone_number;"
        );

    // step 5-2: increase vote number
    public final SQLStmt updateT3Stmt = 
            new SQLStmt("UPDATE T3 SET num_votes = num_votes + 1;");
    
    // step 5-3: 
    public final SQLStmt insertS3Stmt = new SQLStmt(
            "INSERT INTO S3 (phone_number, num_votes) SELECT * FROM T3;");
    
    // delete the content of T3
    //public final SQLStmt deleteT3Stmt = new SQLStmt("DELETE FROM T3;");


    // step 6-1: insert temp table T4
    public final SQLStmt insertT4Stmt = new SQLStmt(
            "INSERT INTO T4 (contestant_number, state, num_votes) SELECT votes_by_contestant_number_state.* FROM votes_by_contestant_number_state, S2 WHERE (votes_by_contestant_number_state.contestant_number=S2.contestant_number) and (votes_by_contestant_number_state.state=S2.state);"
        );

    // step 6-2: increase vote number
    public final SQLStmt updateT4Stmt = 
            new SQLStmt("UPDATE T4 SET num_votes = num_votes + 1");

    // step 6-3: insert temp stream S4
    public final SQLStmt insertS4Stmt = new SQLStmt(
            "INSERT INTO S4 (contestant_number, state, num_votes) SELECT * from T4;");
    
    // detlete the content of T4
    //public final SQLStmt deleteT4Stmt = new SQLStmt("DELETE FROM T4;");
    
    // FIXME, after using the tuple in stream, we should delete it. 
    public final SQLStmt deleteStreamStmt = 
        new SQLStmt("DELETE FROM S2");

}
