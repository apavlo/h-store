package edu.brown.benchmark.streamtrigger.procedures;

import org.voltdb.SQLStmt;
import org.voltdb.StmtInfo;
import org.voltdb.VoltTrigger;

public class SimpleTrigger7 extends VoltTrigger {

    @Override
    protected String toSetStreamName() {
        return "S7";
    }

    public final SQLStmt insertS2 = new SQLStmt(
        "INSERT INTO S8 (value) SELECT * FROM S7;"
    );
    
}
