package edu.brown.benchmark.streamexample.procedures;

import org.voltdb.SQLStmt;
import org.voltdb.VoltProcedure;
import org.voltdb.VoltTable;

public class InsertIntoSelect extends VoltProcedure {
    public final SQLStmt InsertSelect = new SQLStmt("INSERT INTO TABLEC (A_ID, A_VALUE) SELECT * FROM TABLEA");

    public VoltTable[] run() {
    	voltQueueSQL(InsertSelect);
    	return (voltExecuteSQL());
    }
}
