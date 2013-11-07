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
        // S-Store support two ways to fire frontend trigger procedures
        // first way - direct execute such procedure in HStoreSite (Server side)
        // second way - send it back to client to run it (Client side)
        // it means this procedure will be back to client 
        // and let client to explictly call it
        setBeDefault(false); 
        
        // set which stream will be used to trigger this frontend procedure
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