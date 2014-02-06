package edu.brown.benchmark.wordcount.procedures;

import org.voltdb.ProcInfo;
import org.voltdb.SQLStmt;
import org.voltdb.VoltProcedure;
import org.voltdb.VoltTable;

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
    
    public long run(String word) {

        voltQueueSQL(checkWordStmt, word);
        VoltTable validation[] = voltExecuteSQL();

        // initialize the counts table
        if (validation[0].getRowCount() == 0) {
            voltQueueSQL(insertNewWordStmt, word);
            voltExecuteSQL();
        }
        
        voltQueueSQL(insertWords, word);
        voltExecuteSQL(true);

        return 0;
    }
}
