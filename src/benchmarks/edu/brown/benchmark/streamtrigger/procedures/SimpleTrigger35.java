package edu.brown.benchmark.streamtrigger.procedures;

import org.voltdb.SQLStmt;
import org.voltdb.StmtInfo;
import org.voltdb.VoltTrigger;

public class SimpleTrigger35 extends VoltTrigger {

    @Override
    protected String toSetStreamName() {
        return "S35";
    }

    public final SQLStmt insertS2 = new SQLStmt(
        "INSERT INTO S36 (value) SELECT * FROM S35;"
    );
    
}
