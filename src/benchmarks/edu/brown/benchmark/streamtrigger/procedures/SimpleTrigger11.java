package edu.brown.benchmark.streamtrigger.procedures;

import org.voltdb.SQLStmt;
import org.voltdb.StmtInfo;
import org.voltdb.VoltTrigger;

public class SimpleTrigger11 extends VoltTrigger {

    @Override
    protected String toSetStreamName() {
        return "S11";
    }

    public final SQLStmt insertS12 = new SQLStmt(
        "INSERT INTO S12 (value) SELECT * FROM S11;"
    );
    
}
