package edu.brown.benchmark.mapreduce.procedures;

import org.voltdb.ProcInfo;
import org.voltdb.SQLStmt;
import org.voltdb.VoltProcedure;
import org.voltdb.VoltTable;

/**
 * 
 * @author xin
 */
@ProcInfo(
    partitionInfo = "TABLEA.A_ID: 0",
    singlePartition = true
)
public class NormalWordCount extends VoltProcedure {

    public final SQLStmt NameCount = new SQLStmt(
            "SELECT A_NAME, COUNT(*) FROM TABLEA WHERE A_AGE >= ? GROUP BY A_NAME");
   
    /**
     * 
     * @param a_id
     * @return
     */
    public VoltTable[] run(long a_id) {
        voltQueueSQL(NameCount, a_id);
       
        return (voltExecuteSQL());
    }
    
}
