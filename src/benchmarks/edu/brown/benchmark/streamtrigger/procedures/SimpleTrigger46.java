package edu.brown.benchmark.streamtrigger.procedures;

import org.voltdb.SQLStmt;
import org.voltdb.StmtInfo;
import org.voltdb.VoltTrigger;

public class SimpleTrigger46 extends VoltTrigger {

    @Override
    protected String toSetStreamName() {
        return "S46";
    }

    public final SQLStmt insertS2 = new SQLStmt(
        "INSERT INTO S47 (value) SELECT * FROM S46;"
    );
    
}
