package edu.brown.benchmark.streamtrigger.procedures;

import org.voltdb.SQLStmt;
import org.voltdb.StmtInfo;
import org.voltdb.VoltTrigger;

public class SimpleTrigger12 extends VoltTrigger {

    @Override
    protected String toSetStreamName() {
        return "S12";
    }

    public final SQLStmt insertS2 = new SQLStmt(
        "INSERT INTO S13 (value) SELECT * FROM S12;"
    );
    
}
