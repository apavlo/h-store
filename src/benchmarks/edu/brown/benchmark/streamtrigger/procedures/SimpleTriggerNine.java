package edu.brown.benchmark.streamtrigger.procedures;

import org.voltdb.SQLStmt;
import org.voltdb.StmtInfo;
import org.voltdb.VoltTrigger;

public class SimpleTriggerNine extends VoltTrigger {

    @Override
    protected String toSetStreamName() {
        return "S9";
    }

    public final SQLStmt insertS2 = new SQLStmt(
        "INSERT INTO S10 (value) SELECT * FROM S9;"
    );
    
}
