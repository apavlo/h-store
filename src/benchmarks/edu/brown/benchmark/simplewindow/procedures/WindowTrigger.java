package edu.brown.benchmark.simplewindow.procedures;

import org.voltdb.SQLStmt;
import org.voltdb.VoltTrigger;

public class WindowTrigger extends VoltTrigger {

    @Override
    protected String toSetStreamName() {
        return "S1";
    }

    public final SQLStmt insertStmt = new SQLStmt(
        "INSERT INTO W_ROWS (myvalue) SELECT * FROM S1;"
    );
    
}
