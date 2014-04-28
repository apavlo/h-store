package edu.brown.benchmark.voterwintimesstorefullstream.procedures;
import org.voltdb.SQLStmt;
import org.voltdb.StmtInfo;
import org.voltdb.VoltTrigger;

public class ValidateState extends VoltTrigger {

    @Override
    protected String toSetStreamName() {
        return "S1";
    }
    
    

     // step 1: Validate contestants
    public final SQLStmt insertS2Stmt = 
        new SQLStmt("INSERT INTO S2 (vote_id, phone_number, state, contestant_number, time) SELECT S1.vote_id, S1.phone_number, area_code_state.state, S1.contestant_number, S1.time FROM S1, area_code_state WHERE s1.area_code=area_code_state.area_code;");
    
    //public final SQLStmt insertS2pt2Stmt = 
    //       new SQLStmt("INSERT INTO S2 (vote_id, phone_number, state, contestant_number, time) SELECT S1.vote_id, S1.phone_number, s1.state, S1.contestant_number, S1.time FROM S1;");
        
    // FIXME, after using the tuple in stream, we should delete it. 
//    public final SQLStmt deleteVotesStreamStmt = 
//      new SQLStmt("DELETE FROM votes_stream");

}
