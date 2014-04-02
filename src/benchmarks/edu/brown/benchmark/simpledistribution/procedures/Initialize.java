package edu.brown.benchmark.simpledistribution.procedures;

import org.voltdb.ProcInfo;
import org.voltdb.SQLStmt;
import org.voltdb.VoltProcedure;

@ProcInfo (
        singlePartition = false
)
public class Initialize extends VoltProcedure
{
    // Inserts an area code/state mapping
    public final SQLStmt insertStmt = new SQLStmt("INSERT INTO TABLEA VALUES (?,?);");

    
    public long run() {
        for(long i=0; i<10l; i++)
        {
            long id = i;
            String value= Long.toString(i);
            voltQueueSQL(insertStmt, id, value  );
            voltExecuteSQL();
        }
        return 0;
    }
    
}