package edu.brown.benchmark.streamtrigger.procedures;

import org.voltdb.SQLStmt;
import org.voltdb.StmtInfo;
import org.voltdb.VoltTrigger;

public class SimpleTrigger extends VoltTrigger {

    @Override
    protected String toSetStreamName() {
        return "S1";
    }

    public final SQLStmt insertS2 = new SQLStmt(
        "INSERT INTO S2 (value) SELECT * FROM S1;"
    );
    
}
