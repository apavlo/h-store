package edu.brown.benchmark.streamtrigger.procedures;

import org.voltdb.SQLStmt;
import org.voltdb.StmtInfo;
import org.voltdb.VoltTrigger;

public class SimpleTrigger39 extends VoltTrigger {

    @Override
    protected String toSetStreamName() {
        return "S39";
    }

    public final SQLStmt insertS2 = new SQLStmt(
        "INSERT INTO S40 (value) SELECT * FROM S39;"
    );
    
}
