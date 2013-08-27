package edu.brown.benchmark.streamexample.procedures;

import org.voltdb.SQLStmt;
import org.voltdb.VoltProcedure;
import org.voltdb.VoltTable;

public class Initialize extends VoltProcedure {
	public final SQLStmt InsertB = new SQLStmt("INSERT INTO TABLEB (B_ID, NUMROWS) VALUES (1, 0)");
	public final SQLStmt InsertA = new SQLStmt("INSERT INTO TABLEA (A_ID, A_VALUE) VALUES (1, 'AAA')");

    public VoltTable[] run() {
    	voltQueueSQL(InsertB);
    	voltQueueSQL(InsertA);
        return (voltExecuteSQL());
    }
}
