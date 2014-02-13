package edu.brown.benchmark.wordcountsstorewithbatch.procedures;
import org.voltdb.SQLStmt;
import org.voltdb.VoltTrigger;

public class CountTrigger extends VoltTrigger {

    @Override
    protected String toSetStreamName() {
        //return "W_RESULTS";
    	return "W_WORDS";
    }

     // step 1: Validate contestants
    public final SQLStmt insertResultsStmt = 
        //new SQLStmt("INSERT INTO results (word, time, num) SELECT word, max(time), sum(num) FROM W_RESULTS group by word;");
    		new SQLStmt("INSERT INTO results (word, time, num) SELECT word, max(time), count(*) FROM W_WORDS group by word;");
    
    // FIXME, after using the tuple in stream, we should delete it. 
//    public final SQLStmt deleteVotesStreamStmt = 
//      new SQLStmt("DELETE FROM votes_stream");

}
