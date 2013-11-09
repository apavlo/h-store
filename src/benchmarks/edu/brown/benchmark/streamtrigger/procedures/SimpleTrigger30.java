package edu.brown.benchmark.streamtrigger.procedures;

import org.voltdb.SQLStmt;
import org.voltdb.StmtInfo;
import org.voltdb.VoltTrigger;

public class SimpleTrigger30 extends VoltTrigger {

    @Override
    protected String toSetStreamName() {
        return "S30";
    }

    public final SQLStmt insertS2 = new SQLStmt(
        "INSERT INTO S31 (value) SELECT * FROM S30;"
    );
    
}
