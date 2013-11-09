package edu.brown.benchmark.streamtrigger.procedures;

import org.voltdb.SQLStmt;
import org.voltdb.StmtInfo;
import org.voltdb.VoltTrigger;

public class SimpleTrigger38 extends VoltTrigger {

    @Override
    protected String toSetStreamName() {
        return "S38";
    }

    public final SQLStmt insertS2 = new SQLStmt(
        "INSERT INTO S39 (value) SELECT * FROM S38;"
    );
    
}
