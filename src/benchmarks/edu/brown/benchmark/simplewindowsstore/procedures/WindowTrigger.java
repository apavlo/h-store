package edu.brown.benchmark.simplewindowsstore.procedures;

import org.voltdb.SQLStmt;
import org.voltdb.VoltTrigger;

public class WindowTrigger extends VoltTrigger {

    @Override
    protected String toSetStreamName() {
        return "S1";
    }

    public final SQLStmt insertStmt = new SQLStmt(
        "INSERT INTO W_ROWS (myvalue, time) SELECT * FROM S1;"
    );
    
    public final SQLStmt deleteStmt = new SQLStmt(
            "DELETE FROM S1;"
    );
}