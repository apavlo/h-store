package edu.brown.benchmark.streamtrigger.procedures;

import org.voltdb.SQLStmt;
import org.voltdb.StmtInfo;
import org.voltdb.VoltTrigger;

public class SimpleTrigger13 extends VoltTrigger {

    @Override
    protected String toSetStreamName() {
        return "S13";
    }

    public final SQLStmt insertS2 = new SQLStmt(
        "INSERT INTO S14 (value) SELECT * FROM S13;"
    );
    
}
