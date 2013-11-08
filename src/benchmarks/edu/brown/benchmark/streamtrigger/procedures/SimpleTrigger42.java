package edu.brown.benchmark.streamtrigger.procedures;

import org.voltdb.SQLStmt;
import org.voltdb.StmtInfo;
import org.voltdb.VoltTrigger;

public class SimpleTrigger42 extends VoltTrigger {

    @Override
    protected String toSetStreamName() {
        return "S42";
    }

    public final SQLStmt insertS2 = new SQLStmt(
        "INSERT INTO S43 (value) SELECT * FROM S42;"
    );
    
}
