package edu.brown.benchmark.streamexample.procedures;

import org.voltdb.SQLStmt;
import org.voltdb.VoltProcedure;
import org.voltdb.VoltTable;

public class Initialize extends VoltProcedure {
	public final SQLStmt InsertB = new SQLStmt("INSERT INTO TABLEB (B_ID, NUMROWS) VALUES (1, 0)");
	public final SQLStmt InsertA = new SQLStmt("INSERT INTO TABLEA (A_ID, A_VALUE) VALUES (1, 'AAA')");
	public final SQLStmt InsertJ = new SQLStmt("INSERT INTO TABLEJ (J_ID, J_VALUE) VALUES (1, 'JJJ')");
	public final SQLStmt InsertJ2 = new SQLStmt("INSERT INTO TABLEJ (J_ID, J_VALUE) VALUES (1, 'KKK')");

    public VoltTable[] run() {
    	voltQueueSQL(InsertB);
    	voltQueueSQL(InsertA);
    	voltQueueSQL(InsertJ);
    	voltQueueSQL(InsertJ2);
        return (voltExecuteSQL());
    }
}
