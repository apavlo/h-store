package edu.brown.benchmark.streamtrigger.procedures;

import org.voltdb.SQLStmt;
import org.voltdb.StmtInfo;
import org.voltdb.VoltTrigger;

public class SimpleTrigger17 extends VoltTrigger {

    @Override
    protected String toSetStreamName() {
        return "S17";
    }

    public final SQLStmt insertS2 = new SQLStmt(
        "INSERT INTO S18 (value) SELECT * FROM S17;"
    );
    
}
