package edu.brown.benchmark.streamtrigger.procedures;

import org.voltdb.SQLStmt;
import org.voltdb.StmtInfo;
import org.voltdb.VoltTrigger;

public class SimpleTrigger50 extends VoltTrigger {

    @Override
    protected String toSetStreamName() {
        return "S50";
    }

    public final SQLStmt insertS2 = new SQLStmt(
        "INSERT INTO S51 (value) SELECT * FROM S50;"
    );
    
}
