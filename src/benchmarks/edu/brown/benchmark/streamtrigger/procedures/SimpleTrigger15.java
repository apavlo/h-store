package edu.brown.benchmark.streamtrigger.procedures;

import org.voltdb.SQLStmt;
import org.voltdb.StmtInfo;
import org.voltdb.VoltTrigger;

public class SimpleTrigger15 extends VoltTrigger {

    @Override
    protected String toSetStreamName() {
        return "S15";
    }

    public final SQLStmt insertS2 = new SQLStmt(
        "INSERT INTO S16 (value) SELECT * FROM S15;"
    );
    
}
