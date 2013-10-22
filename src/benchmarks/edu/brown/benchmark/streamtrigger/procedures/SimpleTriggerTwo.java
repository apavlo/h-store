package edu.brown.benchmark.streamtrigger.procedures;

import org.voltdb.SQLStmt;
import org.voltdb.StmtInfo;
import org.voltdb.VoltTrigger;

public class SimpleTriggerTwo extends VoltTrigger {

    @Override
    protected String toSetStreamName() {
        return "S2";
    }

    public final SQLStmt insertS2 = new SQLStmt(
        "INSERT INTO S3 (value) SELECT * FROM S2;"
    );
    
}
