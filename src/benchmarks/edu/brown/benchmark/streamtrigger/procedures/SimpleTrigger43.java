package edu.brown.benchmark.streamtrigger.procedures;

import org.voltdb.SQLStmt;
import org.voltdb.StmtInfo;
import org.voltdb.VoltTrigger;

public class SimpleTrigger43 extends VoltTrigger {

    @Override
    protected String toSetStreamName() {
        return "S43";
    }

    public final SQLStmt insertS2 = new SQLStmt(
        "INSERT INTO S44 (value) SELECT * FROM S43;"
    );
    
}
