package edu.brown.benchmark.streamtrigger.procedures;

import org.voltdb.SQLStmt;
import org.voltdb.StmtInfo;
import org.voltdb.VoltTrigger;

public class SimpleTrigger29 extends VoltTrigger {

    @Override
    protected String toSetStreamName() {
        return "S29";
    }

    public final SQLStmt insertS2 = new SQLStmt(
        "INSERT INTO S30 (value) SELECT * FROM S29;"
    );
    
}
