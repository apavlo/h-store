package edu.brown.benchmark.streamtrigger.procedures;

import org.voltdb.SQLStmt;
import org.voltdb.StmtInfo;
import org.voltdb.VoltTrigger;

public class SimpleTrigger47 extends VoltTrigger {

    @Override
    protected String toSetStreamName() {
        return "S47";
    }

    public final SQLStmt insertS2 = new SQLStmt(
        "INSERT INTO S48 (value) SELECT * FROM S47;"
    );
    
}
