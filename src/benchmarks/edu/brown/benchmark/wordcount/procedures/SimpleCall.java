package edu.brown.benchmark.wordcount.procedures;

import java.util.List;

import org.voltdb.ProcInfo;
import org.voltdb.SQLStmt;
import org.voltdb.VoltProcedure;
import org.voltdb.VoltTable;

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
        
        for(Tuple tuple : tuples)
        {
            word = (String)tuple.getField("WORD");
            voltQueueSQL(insertWords, word);
        }
        voltExecuteSQL(true);

        return 0;
    }
    
}
