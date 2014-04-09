package edu.brown.benchmark.simpledistribution.procedures;

import org.voltdb.ProcInfo;
import org.voltdb.SQLStmt;
import org.voltdb.VoltProcedure;

@ProcInfo (
        singlePartition = true
    )
public class SP6 extends VoltProcedure {
    
    protected void toSetTriggerTableName()
    {   
        addTriggerTable("S3");
    }

    public final SQLStmt insertS6 = new SQLStmt("INSERT INTO S6 (value) SELECT * FROM S3;");
    
    public final SQLStmt deleteS3 = new SQLStmt("DELETE FROM S3;");
    
    public long run() {

        voltQueueSQL(insertS6);
        voltExecuteSQL();
        
        voltQueueSQL(deleteS3);
        voltExecuteSQL();

        return 0;

    }

}
