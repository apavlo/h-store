package edu.brown.benchmark.frontendtrigger.procedures;

import org.voltdb.ProcInfo;
import org.voltdb.SQLStmt;
import org.voltdb.VoltProcedure;
import org.voltdb.VoltTable;
import org.voltdb.types.TimestampType;

@ProcInfo (
        singlePartition = true
    )
public class AnotherCall extends VoltProcedure {
    
    protected void toSetTriggerTableName()
    {        
		addTriggerTable("S1");
    }

    public final SQLStmt insertS2 = new SQLStmt(
            "INSERT INTO S2 (myvalue) SELECT * FROM S1;"
        );

    public final SQLStmt deleteS1 = new SQLStmt(
            "DELETE FROM S1;"
        );

	public long run() {

        voltQueueSQL(insertS2);
        voltExecuteSQL();

        voltQueueSQL(deleteS1);
        voltExecuteSQL();

		return 0;
    }
}