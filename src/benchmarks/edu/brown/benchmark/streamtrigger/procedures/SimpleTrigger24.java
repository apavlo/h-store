package edu.brown.benchmark.streamtrigger.procedures;

import org.voltdb.SQLStmt;
import org.voltdb.StmtInfo;
import org.voltdb.VoltTrigger;

public class SimpleTrigger24 extends VoltTrigger {

    @Override
    protected String toSetStreamName() {
        return "S24";
    }

    public final SQLStmt insertS2 = new SQLStmt(
        "INSERT INTO S25 (value) SELECT * FROM S24;"
    );
    
}
