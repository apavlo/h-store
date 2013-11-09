package edu.brown.benchmark.streamtrigger.procedures;

import org.voltdb.SQLStmt;
import org.voltdb.StmtInfo;
import org.voltdb.VoltTrigger;

public class SimpleTrigger41 extends VoltTrigger {

    @Override
    protected String toSetStreamName() {
        return "S41";
    }

    public final SQLStmt insertS2 = new SQLStmt(
        "INSERT INTO S42 (value) SELECT * FROM S41;"
    );
    
}
