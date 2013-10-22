package edu.brown.benchmark.streamtrigger.procedures;

import org.voltdb.SQLStmt;
import org.voltdb.StmtInfo;
import org.voltdb.VoltTrigger;

public class SimpleTriggerFive extends VoltTrigger {

    @Override
    protected String toSetStreamName() {
        return "S5";
    }

    public final SQLStmt insertS2 = new SQLStmt(
        "INSERT INTO S6 (value) SELECT * FROM S5;"
    );
    
}
