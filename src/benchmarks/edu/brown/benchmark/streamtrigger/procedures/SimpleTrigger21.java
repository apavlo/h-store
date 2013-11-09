package edu.brown.benchmark.streamtrigger.procedures;

import org.voltdb.SQLStmt;
import org.voltdb.StmtInfo;
import org.voltdb.VoltTrigger;

public class SimpleTrigger21 extends VoltTrigger {

    @Override
    protected String toSetStreamName() {
        return "S21";
    }

    public final SQLStmt insertS2 = new SQLStmt(
        "INSERT INTO S22 (value) SELECT * FROM S21;"
    );
    
}
