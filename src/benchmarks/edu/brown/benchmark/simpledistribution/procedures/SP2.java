package edu.brown.benchmark.simpledistribution.procedures;

import org.voltdb.ProcInfo;
import org.voltdb.SQLStmt;
import org.voltdb.VoltProcedure;

@ProcInfo (
        singlePartition = true
    )
public class SP2 extends VoltProcedure {
    
    protected void toSetTriggerTableName()
    {   
        addTriggerTable("S1");
    }

    public final SQLStmt insertS2 = new SQLStmt("INSERT INTO S2 (value) SELECT * FROM S1;");
    
    public final SQLStmt deleteS1 = new SQLStmt("DELETE FROM S1;");
    
    public long run() {

        voltQueueSQL(insertS2);
        voltExecuteSQL();
        
//        voltQueueSQL(deleteS1);
//        voltExecuteSQL();

        return 0;

    }

}
