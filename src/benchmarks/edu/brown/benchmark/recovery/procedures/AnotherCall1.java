package edu.brown.benchmark.recovery.procedures;

import org.voltdb.ProcInfo;
import org.voltdb.SQLStmt;
import org.voltdb.VoltProcedure;
import org.voltdb.VoltTable;
import org.voltdb.types.TimestampType;

@ProcInfo (
        singlePartition = true
    )
public class AnotherCall1 extends VoltProcedure {
    
    protected void toSetTriggerTableName()
    {   
        // set which stream will be used to trigger this frontend procedure
		addTriggerTable("S1");
    }

    public final SQLStmt insertS2 = new SQLStmt("INSERT INTO S2 (value) SELECT * FROM S1;");

    // delete statements
    public final SQLStmt deleteS1 = new SQLStmt("DELETE FROM S1;");

	public long run() {

        voltQueueSQL(insertS2);
        voltExecuteSQL();
        
        //delete
        voltQueueSQL(deleteS1);
        voltExecuteSQL();

		return 0;
    }
}