package edu.brown.benchmark.streamtrigger.procedures;

import org.voltdb.SQLStmt;
import org.voltdb.StmtInfo;
import org.voltdb.VoltTrigger;

public class SimpleTrigger45 extends VoltTrigger {

    @Override
    protected String toSetStreamName() {
        return "S45";
    }

    public final SQLStmt insertS2 = new SQLStmt(
        "INSERT INTO S46 (value) SELECT * FROM S45;"
    );
    
}
