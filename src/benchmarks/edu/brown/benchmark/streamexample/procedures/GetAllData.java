package edu.brown.benchmark.streamexample.procedures;

import org.voltdb.SQLStmt;
import org.voltdb.VoltProcedure;
import org.voltdb.VoltTable;

public class GetAllData extends VoltProcedure {
    public final SQLStmt GetA = new SQLStmt("SELECT * FROM TABLEA");

    public VoltTable[] run() {
        voltQueueSQL(GetA);
        return (voltExecuteSQL());
    }
}
