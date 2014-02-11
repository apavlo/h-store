package edu.brown.benchmark.wordcounthstore.procedures;

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
            "INSERT INTO counts (word, num, time) VALUES (?, 1, ?);"
        );
    
    public final SQLStmt addWord = new SQLStmt(
    		"UPDATE counts SET num = num + 1 WHERE word=?;"
        );
    
    public long run(String word, int time) {

        voltQueueSQL(checkWordStmt, word);
        VoltTable validation[] = voltExecuteSQL();
        
        // initialize the counts table
        if (validation[0].getRowCount() == 0) {
            voltQueueSQL(insertNewWordStmt, word, time);
        }
        else {
        	voltQueueSQL(addWord, word);
        }
        voltExecuteSQL(true);

        return 0;
    }
}
