package edu.brown.benchmark.streamtrigger.procedures;

import org.voltdb.SQLStmt;
import org.voltdb.StmtInfo;
import org.voltdb.VoltTrigger;

public class SimpleTriggerFour extends VoltTrigger {

    @Override
    protected String toSetStreamName() {
        return "S4";
    }

    public final SQLStmt insertS2 = new SQLStmt(
        "INSERT INTO S5 (value) SELECT * FROM S4;"
    );
    
}
