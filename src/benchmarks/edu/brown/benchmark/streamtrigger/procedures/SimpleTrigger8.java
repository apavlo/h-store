package edu.brown.benchmark.streamtrigger.procedures;

import org.voltdb.SQLStmt;
import org.voltdb.StmtInfo;
import org.voltdb.VoltTrigger;

public class SimpleTrigger8 extends VoltTrigger {

    @Override
    protected String toSetStreamName() {
        return "S8";
    }

    public final SQLStmt insertS2 = new SQLStmt(
        "INSERT INTO S9 (value) SELECT * FROM S8;"
    );
    
}
