package edu.brown.benchmark.streamtrigger.procedures;

import org.voltdb.SQLStmt;
import org.voltdb.StmtInfo;
import org.voltdb.VoltTrigger;

public class SimpleTrigger20 extends VoltTrigger {

    @Override
    protected String toSetStreamName() {
        return "S20";
    }

    public final SQLStmt insertS2 = new SQLStmt(
        "INSERT INTO S21 (value) SELECT * FROM S20;"
    );
    
}
