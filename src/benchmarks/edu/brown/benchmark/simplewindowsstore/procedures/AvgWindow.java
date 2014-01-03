package edu.brown.benchmark.simplewindowsstore.procedures;

import org.voltdb.SQLStmt;
import org.voltdb.VoltTrigger;

public class AvgWindow extends VoltTrigger {
	@Override
    protected String toSetStreamName() {
        return "W_ROWS";
    }

    public final SQLStmt insertStmt = new SQLStmt(
        "INSERT INTO AVG_FROM_WIN (time, valAvg) SELECT MAX(time), AVG(myvalue) FROM W_ROWS;"
    );
}
