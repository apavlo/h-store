package edu.brown.benchmark.simpledistribution.procedures;

import org.voltdb.ProcInfo;
import org.voltdb.SQLStmt;
import org.voltdb.VoltProcedure;

@ProcInfo (
        singlePartition = true
    )
public class SP5 extends VoltProcedure {
    
    protected void toSetTriggerTableName()
    {   
        addTriggerTable("S2");
    }

    public final SQLStmt insertS5 = new SQLStmt("INSERT INTO S5 (value) SELECT * FROM S2;");
    
    public final SQLStmt deleteS2 = new SQLStmt("DELETE FROM S2;");
    
    public long run() {

        voltQueueSQL(insertS5);
        voltExecuteSQL();
        
        voltQueueSQL(deleteS2);
        voltExecuteSQL();

        return 0;

    }

}
