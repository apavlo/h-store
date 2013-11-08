package edu.brown.benchmark.streamtrigger.procedures;

import org.voltdb.SQLStmt;
import org.voltdb.StmtInfo;
import org.voltdb.VoltTrigger;

public class SimpleTrigger27 extends VoltTrigger {

    @Override
    protected String toSetStreamName() {
        return "S27";
    }

    public final SQLStmt insertS2 = new SQLStmt(
        "INSERT INTO S28 (value) SELECT * FROM S27;"
    );
    
}
