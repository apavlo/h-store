package edu.brown.benchmark.streamtrigger.procedures;

import org.voltdb.SQLStmt;
import org.voltdb.StmtInfo;
import org.voltdb.VoltTrigger;

public class SimpleTrigger16 extends VoltTrigger {

    @Override
    protected String toSetStreamName() {
        return "S16";
    }

    public final SQLStmt insertS2 = new SQLStmt(
        "INSERT INTO S17 (value) SELECT * FROM S16;"
    );
    
}
