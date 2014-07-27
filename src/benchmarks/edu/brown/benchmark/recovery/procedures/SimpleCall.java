package edu.brown.benchmark.recovery.procedures;

import org.voltdb.ProcInfo;
import org.voltdb.SQLStmt;
import org.voltdb.VoltProcedure;
import org.voltdb.VoltTable;
import org.voltdb.types.TimestampType;

@ProcInfo(singlePartition = true)
public class SimpleCall extends VoltProcedure {

    public final SQLStmt insertS1 = new SQLStmt("INSERT INTO S1 (value) VALUES (?);");

    public final SQLStmt insertW1 = new SQLStmt("INSERT INTO W1 (value) VALUES (?);");

    public long run(long number) {

        voltQueueSQL(insertS1, number);
        //voltExecuteSQL();
        
//        voltQueueSQL(insertW1, number);
        voltExecuteSQL();

        return 0;
    }
}
