package edu.brown.benchmark.streamtrigger.procedures;

import org.voltdb.SQLStmt;
import org.voltdb.StmtInfo;
import org.voltdb.VoltTrigger;

public class SimpleTriggerThree extends VoltTrigger {

    @Override
    protected String toSetStreamName() {
        return "S3";
    }

    public final SQLStmt insertS2 = new SQLStmt(
        "INSERT INTO S4 (value) SELECT * FROM S3;"
    );
    
}
