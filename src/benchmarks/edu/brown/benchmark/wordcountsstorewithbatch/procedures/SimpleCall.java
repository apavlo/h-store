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
    
    //public long run(String word, int time) 
    public long run(String batchJSONString0, String batchJSONString1, String batchJSONString2 , String batchJSONString3, String batchJSONString4)
    {
        
        Batch objBatch = new Batch();
        objBatch.fromJSONString(batchJSONString0);
        
        List<Tuple> tuples = objBatch.getTuples();
        
        objBatch = new Batch();
        objBatch.fromJSONString(batchJSONString1);
        tuples.addAll(objBatch.getTuples());
        
        objBatch = new Batch();
        objBatch.fromJSONString(batchJSONString2);
        tuples.addAll(objBatch.getTuples());

        objBatch = new Batch();
        objBatch.fromJSONString(batchJSONString3);
        tuples.addAll(objBatch.getTuples());

        objBatch = new Batch();
        objBatch.fromJSONString(batchJSONString4);
        tuples.addAll(objBatch.getTuples());

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
                {
                    beFinalBatch = true;
                }
                                
                voltExecuteSQL(beFinalBatch);
                i = 0;
            }
            
        }
        
        
        if( i !=0 )
        {
        	voltExecuteSQL(true);
        }

        return 0;
    }
}
