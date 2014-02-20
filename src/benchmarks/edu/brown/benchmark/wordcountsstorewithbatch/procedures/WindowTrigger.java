package edu.brown.benchmark.wordcountsstorewithbatch.procedures;
import org.voltdb.SQLStmt;
import org.voltdb.VoltTrigger;

public class WindowTrigger extends VoltTrigger {

    @Override
    protected String toSetStreamName() {
        return "words";
    }

     // step 1: Validate contestants
    public final SQLStmt insertW1Stmt = 
        new SQLStmt("INSERT INTO W_WORDS SELECT * FROM words;");
    
    //public final SQLStmt insertpermanent = 
    //        new SQLStmt("INSERT INTO words_full SELECT * FROM words;");

    
    
    // FIXME, after using the tuple in stream, we should delete it. 
//    public final SQLStmt deleteVotesStreamStmt = 
//      new SQLStmt("DELETE FROM votes_stream");

}
