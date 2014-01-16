package edu.brown.benchmark.simplestreamtrigger.procedures;

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
            "INSERT INTO S1 (value) VALUES (?);"
        );
    
    public long run(long value) {

        voltQueueSQL(insertS1, value);
        voltExecuteSQL(true);

        return 0;
    }
}
