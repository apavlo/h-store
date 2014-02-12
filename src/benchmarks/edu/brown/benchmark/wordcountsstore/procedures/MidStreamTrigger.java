package edu.brown.benchmark.wordcountsstore.procedures;
import org.voltdb.SQLStmt;
import org.voltdb.VoltTrigger;

public class MidStreamTrigger extends VoltTrigger {

    @Override
    protected String toSetStreamName() {
        return "W_WORDS";
    }

     // step 1: Validate contestants
    public final SQLStmt insertS2Stmt = 
        new SQLStmt("INSERT INTO midstream (word, time, num) SELECT word, max(time), count(*) FROM W_WORDS group by word;");
    
    // FIXME, after using the tuple in stream, we should delete it. 
//    public final SQLStmt deleteVotesStreamStmt = 
//      new SQLStmt("DELETE FROM votes_stream");

}
