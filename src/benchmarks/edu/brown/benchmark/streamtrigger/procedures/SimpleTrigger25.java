package edu.brown.benchmark.streamtrigger.procedures;

import org.voltdb.SQLStmt;
import org.voltdb.StmtInfo;
import org.voltdb.VoltTrigger;

public class SimpleTrigger25 extends VoltTrigger {

    @Override
    protected String toSetStreamName() {
        return "S25";
    }

    public final SQLStmt insertS2 = new SQLStmt(
        "INSERT INTO S26 (value) SELECT * FROM S25;"
    );
    
}
