package edu.brown.benchmark.wordcountsstorewithbatch.procedures;

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
public class SimpleCall extends VoltProcedure {
    
    public final SQLStmt insertWordStmt = new SQLStmt(
            "INSERT INTO words VALUES (?, ?);"
    		//"INSERT INTO W_WORDS VALUES (?, ?);"
        );
    
    public final SQLStmt selectResultsStmt = new SQLStmt(
            "SELECT word, sum(num) FROM W_RESULTS group by word;"
    		//"INSERT INTO W_WORDS VALUES (?, ?);"
        );
    
    //public long run(String word, int time) 
    public long run(String batchJSONString)
    {
        
        Batch objBatch = new Batch();
        objBatch.fromJSONString(batchJSONString);
        
        List<Tuple> tuples = objBatch.getTuples();
        
        long bid    = objBatch.getID();
        long btime  = objBatch.getTimestamp();

        int maximum = HStoreConf.singleton().site.planner_max_batch_size;
        
        int i = 0;
        int j = 0;
        int size = tuples.size();
        boolean beFinalBatch = false;
        
        for(Tuple tuple : tuples)
        {
            String word = (String)tuple.getField("WORD");
            voltQueueSQL(insertWordStmt, word, bid);
            
            i++;
            j++;
            if( i == maximum )
            {
                if( j == size )
                    beFinalBatch = true;
                                
                voltExecuteSQL(beFinalBatch);
                i = 0;
            }
        }
        
        //voltQueueSQL(selectResultsStmt);
        if( i !=0 )
        voltExecuteSQL(true);

        return 0;
    }
}
