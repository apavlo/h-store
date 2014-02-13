package edu.brown.benchmark.wordcountsstorewithbatch.procedures;
import org.voltdb.SQLStmt;
import org.voltdb.VoltTrigger;

public class ResultsWinTrigger extends VoltTrigger {

    @Override
    protected String toSetStreamName() {
        return "midstream";
    	//return "W_WORDS";
    }

     // step 1: Validate contestants
    public final SQLStmt insertW2Stmt = 
        new SQLStmt("INSERT INTO W_RESULTS (word, time, num) SELECT word, time, num FROM midstream;");
    	//new SQLStmt("INSERT INTO W_RESULTS (word, time, num) SELECT word, time, num FROM W_WORDS;");
    
    // FIXME, after using the tuple in stream, we should delete it. 
//    public final SQLStmt deleteVotesStreamStmt = 
//      new SQLStmt("DELETE FROM votes_stream");

}
