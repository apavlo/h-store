package edu.brown.benchmark.streamtrigger.procedures;

import org.voltdb.SQLStmt;
import org.voltdb.StmtInfo;
import org.voltdb.VoltTrigger;

public class SimpleTrigger31 extends VoltTrigger {

    @Override
    protected String toSetStreamName() {
        return "S31";
    }

    public final SQLStmt insertS2 = new SQLStmt(
        "INSERT INTO S32 (value) SELECT * FROM S31;"
    );
    
}
