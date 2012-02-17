package edu.brown.benchmark.example.procedures;

import org.voltdb.SQLStmt;
import org.voltdb.VoltProcedure;
import org.voltdb.VoltTable;

public class GetData extends VoltProcedure {
    public final SQLStmt GetA = new SQLStmt("SELECT * FROM TABLEA WHERE A_ID = ? ");

    public VoltTable[] run(long a_id) {
        voltQueueSQL(GetA, a_id);
        return (voltExecuteSQL());
    }
}
