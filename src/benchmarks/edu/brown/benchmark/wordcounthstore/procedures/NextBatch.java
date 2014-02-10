package edu.brown.benchmark.wordcounthstore.procedures;

//import org.voltdb.ProcInfo;
import org.voltdb.SQLStmt;
import org.voltdb.VoltProcedure;
//import org.voltdb.VoltTable;

public class NextBatch extends VoltProcedure {
    
    public final SQLStmt updateAgg = new SQLStmt(
            "INSERT INTO persecond SELECT * FROM counts;"
        );
    
    public final SQLStmt deleteCounts = new SQLStmt(
            "DELETE FROM counts;"
        );
    
    public long run() {

        voltQueueSQL(updateAgg);
        voltQueueSQL(deleteCounts);
        voltExecuteSQL(true);

        return 0;
    }
}
