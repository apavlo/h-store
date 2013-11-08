package edu.brown.benchmark.streamtrigger.procedures;

import org.voltdb.SQLStmt;
import org.voltdb.StmtInfo;
import org.voltdb.VoltTrigger;

public class SimpleTrigger49 extends VoltTrigger {

    @Override
    protected String toSetStreamName() {
        return "S49";
    }

    public final SQLStmt insertS2 = new SQLStmt(
        "INSERT INTO S50 (value) SELECT * FROM S49;"
    );
    
}
