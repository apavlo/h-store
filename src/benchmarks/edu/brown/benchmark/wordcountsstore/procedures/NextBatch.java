package edu.brown.benchmark.wordcountsstore.procedures;

//import org.voltdb.ProcInfo;
import org.voltdb.SQLStmt;
import org.voltdb.VoltProcedure;
//import org.voltdb.VoltTable;
import org.voltdb.VoltTable;

public class NextBatch extends VoltProcedure {
	
	private static final int slideSize = 15; //a single timestamp is 2 seconds
	
	public final SQLStmt deleteCounts = new SQLStmt(
            "DELETE FROM counts WHERE time < ?;"
        );
    
    public final SQLStmt updateAgg = new SQLStmt(
            "SELECT word, num, time FROM counts;"
        );
    
    
    
    public long run(int time) {

        voltQueueSQL(deleteCounts, time - slideSize);
        voltQueueSQL(updateAgg);
        VoltTable validation[] = voltExecuteSQL();
        
        System.out.println("Count " + (time-1) + ": " + validation[0].getLong(1));

        return 0;
    }
}
