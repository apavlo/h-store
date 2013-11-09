package edu.brown.benchmark.streamtrigger.procedures;

import org.voltdb.SQLStmt;
import org.voltdb.StmtInfo;
import org.voltdb.VoltTrigger;

public class SimpleTrigger28 extends VoltTrigger {

    @Override
    protected String toSetStreamName() {
        return "S28";
    }

    public final SQLStmt insertS2 = new SQLStmt(
        "INSERT INTO S29 (value) SELECT * FROM S28;"
    );
    
}
