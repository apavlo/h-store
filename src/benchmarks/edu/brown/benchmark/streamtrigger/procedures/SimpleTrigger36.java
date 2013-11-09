package edu.brown.benchmark.streamtrigger.procedures;

import org.voltdb.SQLStmt;
import org.voltdb.StmtInfo;
import org.voltdb.VoltTrigger;

public class SimpleTrigger36 extends VoltTrigger {

    @Override
    protected String toSetStreamName() {
        return "S36";
    }

    public final SQLStmt insertS2 = new SQLStmt(
        "INSERT INTO S37 (value) SELECT * FROM S36;"
    );
    
}
