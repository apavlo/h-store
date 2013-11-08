package edu.brown.benchmark.streamtrigger.procedures;

import org.voltdb.SQLStmt;
import org.voltdb.StmtInfo;
import org.voltdb.VoltTrigger;

public class SimpleTrigger6 extends VoltTrigger {

    @Override
    protected String toSetStreamName() {
        return "S6";
    }

    public final SQLStmt insertS2 = new SQLStmt(
        "INSERT INTO S7 (value) SELECT * FROM S6;"
    );
    
}
