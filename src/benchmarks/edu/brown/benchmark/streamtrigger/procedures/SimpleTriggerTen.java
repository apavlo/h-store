package edu.brown.benchmark.streamtrigger.procedures;

import org.voltdb.SQLStmt;
import org.voltdb.StmtInfo;
import org.voltdb.VoltTrigger;

public class SimpleTriggerTen extends VoltTrigger {

    @Override
    protected String toSetStreamName() {
        return "S10";
    }

    public final SQLStmt insertS2 = new SQLStmt(
        "INSERT INTO S11 (value) SELECT * FROM S10;"
    );
    
}
