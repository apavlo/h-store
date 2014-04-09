package edu.brown.benchmark.simpledistribution.procedures;

import org.voltdb.ProcInfo;
import org.voltdb.SQLStmt;
import org.voltdb.VoltProcedure;

@ProcInfo (
        singlePartition = true
    )
public class SP3 extends VoltProcedure {
    
    protected void toSetTriggerTableName()
    {   
        addTriggerTable("S1");
    }

    public final SQLStmt insertS3 = new SQLStmt("INSERT INTO S3 (value) SELECT * FROM S1;");
    
    public final SQLStmt deleteS1 = new SQLStmt("DELETE FROM S1;");
    
    public long run() {

        voltQueueSQL(insertS3);
        voltExecuteSQL();
        
        voltQueueSQL(deleteS1);
        voltExecuteSQL();

        return 0;

    }

}
