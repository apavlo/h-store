package edu.brown.benchmark.streamtrigger.procedures;

import org.voltdb.SQLStmt;
import org.voltdb.StmtInfo;
import org.voltdb.VoltTrigger;

public class SimpleTrigger14 extends VoltTrigger {

    @Override
    protected String toSetStreamName() {
        return "S14";
    }

    public final SQLStmt insertS2 = new SQLStmt(
        "INSERT INTO S15 (value) SELECT * FROM S14;"
    );
    
}
