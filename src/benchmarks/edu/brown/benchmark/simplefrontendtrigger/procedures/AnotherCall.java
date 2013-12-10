package edu.brown.benchmark.simplefrontendtrigger.procedures;

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
        // set which stream will be used to trigger this frontend procedure
	addTriggerTable("S1");
    }

    //public final SQLStmt insertS2 = new SQLStmt( "INSERT INTO S2 (myvalue) SELECT * FROM S1;" );

     public long run() {
		return 0;
    }
}