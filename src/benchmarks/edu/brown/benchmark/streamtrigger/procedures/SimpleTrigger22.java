package edu.brown.benchmark.streamtrigger.procedures;

import org.voltdb.SQLStmt;
import org.voltdb.StmtInfo;
import org.voltdb.VoltTrigger;

public class SimpleTrigger22 extends VoltTrigger {

    @Override
    protected String toSetStreamName() {
        return "S22";
    }

    public final SQLStmt insertS2 = new SQLStmt(
        "INSERT INTO S23 (value) SELECT * FROM S22;"
    );
    
}
