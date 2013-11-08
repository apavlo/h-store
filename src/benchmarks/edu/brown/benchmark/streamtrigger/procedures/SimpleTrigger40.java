package edu.brown.benchmark.streamtrigger.procedures;

import org.voltdb.SQLStmt;
import org.voltdb.StmtInfo;
import org.voltdb.VoltTrigger;

public class SimpleTrigger40 extends VoltTrigger {

    @Override
    protected String toSetStreamName() {
        return "S40";
    }

    public final SQLStmt insertS2 = new SQLStmt(
        "INSERT INTO S41 (value) SELECT * FROM S40;"
    );
    
}
