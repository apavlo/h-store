package edu.brown.benchmark.streamtrigger.procedures;

import org.voltdb.SQLStmt;
import org.voltdb.StmtInfo;
import org.voltdb.VoltTrigger;

public class SimpleTrigger18 extends VoltTrigger {

    @Override
    protected String toSetStreamName() {
        return "S18";
    }

    public final SQLStmt insertS2 = new SQLStmt(
        "INSERT INTO S19 (value) SELECT * FROM S18;"
    );
    
}
