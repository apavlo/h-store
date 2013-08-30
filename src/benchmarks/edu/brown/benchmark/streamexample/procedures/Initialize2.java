package edu.brown.benchmark.streamexample.procedures;

import org.voltdb.SQLStmt;
import org.voltdb.VoltProcedure;
import org.voltdb.VoltTable;

public class Initialize2 extends VoltProcedure {
	
	public final SQLStmt JoinBandJ = new SQLStmt("SELECT B.B_ID, J.J_VALUE FROM TABLEB AS B INNER JOIN TABLEJ AS J ON B.B_ID = J.J_ID");

    public VoltTable[] run() {
        voltQueueSQL(JoinBandJ);
        return (voltExecuteSQL());
    }
}
