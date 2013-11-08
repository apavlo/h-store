package edu.brown.benchmark.streamtrigger.procedures;

import org.voltdb.SQLStmt;
import org.voltdb.StmtInfo;
import org.voltdb.VoltTrigger;

public class SimpleTrigger19 extends VoltTrigger {

    @Override
    protected String toSetStreamName() {
        return "S19";
    }

    public final SQLStmt insertS2 = new SQLStmt(
        "INSERT INTO S20 (value) SELECT * FROM S19;"
    );
    
}
