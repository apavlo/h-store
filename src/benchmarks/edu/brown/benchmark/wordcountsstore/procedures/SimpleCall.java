package edu.brown.benchmark.wordcountsstore.procedures;

import org.voltdb.ProcInfo;
import org.voltdb.SQLStmt;
import org.voltdb.VoltProcedure;
import org.voltdb.VoltTable;

@ProcInfo (
        singlePartition = true
    )
public class SimpleCall extends VoltProcedure {
    
    public final SQLStmt insertWordStmt = new SQLStmt(
            "INSERT INTO words VALUES (?, ?);"
        );
    
    public long run(String word, int time) {

        voltQueueSQL(insertWordStmt, word, time);
        voltExecuteSQL(true);

        return 0;
    }
}
