package edu.brown.benchmark.streamtrigger.procedures;

import org.voltdb.SQLStmt;
import org.voltdb.StmtInfo;
import org.voltdb.VoltTrigger;

public class SimpleTrigger33 extends VoltTrigger {

    @Override
    protected String toSetStreamName() {
        return "S33";
    }

    public final SQLStmt insertS2 = new SQLStmt(
        "INSERT INTO S34 (value) SELECT * FROM S33;"
    );
    
}
