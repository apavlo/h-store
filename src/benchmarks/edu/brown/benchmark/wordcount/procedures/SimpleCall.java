package edu.brown.benchmark.wordcount.procedures;

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
    
    public final SQLStmt checkWordStmt = new SQLStmt(
            "SELECT word FROM counts WHERE word = ?;"
        );
    
    public final SQLStmt insertNewWordStmt = new SQLStmt(
            "INSERT INTO counts (word, num) VALUES (?, 0);"
        );
    
    public final SQLStmt insertWords = new SQLStmt(
            "INSERT INTO words (word) VALUES (?);"
        );
    
    public long run(String batchJSONString) {

        System.out.println(batchJSONString);

        Batch objBatch = new Batch();
        objBatch.fromJSONString(batchJSONString);
        
        List<Tuple> tuples = objBatch.getTuples();
        
        long bid = objBatch.getID();
        String word = "";

        for(Tuple tuple : tuples)
        {
            word = (String)tuple.getField("WORD");
            
            voltQueueSQL(checkWordStmt, word);
            VoltTable validation[] = voltExecuteSQL();
    
            // initialize the counts table
            if (validation[0].getRowCount() == 0) {
                voltQueueSQL(insertNewWordStmt, word);
                voltExecuteSQL();
            }

        }
        
        int maximum = HStoreConf.singleton().site.planner_max_batch_size;
        
        int i = 0;
        int j = 0;
        int size = tuples.size();
        boolean beFinalBatch = false;
        
        for(Tuple tuple : tuples)
        {
            word = (String)tuple.getField("WORD");
            voltQueueSQL(insertWords, word);
            
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
        
        if( i !=0 )
            voltExecuteSQL(true);

        return 0;
    }
    
}
