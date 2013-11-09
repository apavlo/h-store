package edu.brown.benchmark.streamtrigger.procedures;

import org.voltdb.SQLStmt;
import org.voltdb.StmtInfo;
import org.voltdb.VoltTrigger;

public class SimpleTrigger26 extends VoltTrigger {

    @Override
    protected String toSetStreamName() {
        return "S26";
    }

    public final SQLStmt insertS2 = new SQLStmt(
        "INSERT INTO S27 (value) SELECT * FROM S26;"
    );
    
}
