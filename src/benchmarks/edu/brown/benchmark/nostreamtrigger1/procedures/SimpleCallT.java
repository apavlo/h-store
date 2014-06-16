package edu.brown.benchmark.nostreamtrigger1.procedures;

import org.voltdb.ProcInfo;
import org.voltdb.SQLStmt;
import org.voltdb.VoltProcedure;
import org.voltdb.VoltTable;
import org.voltdb.types.TimestampType;

@ProcInfo(singlePartition = true)
public class SimpleCallT extends VoltProcedure {

    public final SQLStmt insertT1 = new SQLStmt("INSERT INTO T1 (value) VALUES (?);");


    public long run(long number) {

        voltQueueSQL(insertT1, number);
        voltExecuteSQL();

        return 0;
    }
}
