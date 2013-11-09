package edu.brown.benchmark.streamtrigger.procedures;

import org.voltdb.SQLStmt;
import org.voltdb.StmtInfo;
import org.voltdb.VoltTrigger;

public class SimpleTrigger23 extends VoltTrigger {

    @Override
    protected String toSetStreamName() {
        return "S23";
    }

    public final SQLStmt insertS2 = new SQLStmt(
        "INSERT INTO S24 (value) SELECT * FROM S23;"
    );
    
}
