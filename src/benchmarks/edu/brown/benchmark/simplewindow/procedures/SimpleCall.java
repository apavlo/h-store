package edu.brown.benchmark.simplewindow.procedures;

import org.voltdb.ProcInfo;
import org.voltdb.SQLStmt;
import org.voltdb.VoltProcedure;
import org.voltdb.VoltTable;
import org.voltdb.types.TimestampType;

@ProcInfo (
        singlePartition = true
    )
public class SimpleCall extends VoltProcedure {
    
    public final SQLStmt insertS1 = new SQLStmt(
            "INSERT INTO S1 (myvalue) VALUES (0);"
        );

    public long run() {

        voltQueueSQL(insertS1);
        voltExecuteSQL();

		return 0;
    }
}
