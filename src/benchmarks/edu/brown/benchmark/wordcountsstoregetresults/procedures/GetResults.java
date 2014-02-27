package edu.brown.benchmark.wordcountsstoregetresults.procedures;

import java.util.List;

import org.voltdb.ProcInfo;
import org.voltdb.SQLStmt;
import org.voltdb.VoltProcedure;
import org.voltdb.VoltTable;

import edu.brown.hstore.conf.HStoreConf;
import edu.brown.stream.Batch;
import edu.brown.stream.Tuple;

@ProcInfo (
        singlePartition = true
    )
public class GetResults extends VoltProcedure {
    
    public final SQLStmt selectResultsStmt = new SQLStmt(
            //"SELECT word, sum(num) FROM W_WORDS GROUP BY word ORDER BY word ASC;"
    		"SELECT word, sum(num) FROM W_RESULTS GROUP BY word ORDER BY word ASC;"
        );
    
    //public long run(String word, int time) 
    public VoltTable[] run()
    {
       VoltTable[] results;
       voltQueueSQL(selectResultsStmt);
       results = voltExecuteSQL(true);

       return results;
    }
}
