package edu.brown.benchmark.streamtrigger.procedures;

import org.voltdb.SQLStmt;
import org.voltdb.StmtInfo;
import org.voltdb.VoltTrigger;

public class SimpleTrigger32 extends VoltTrigger {

    @Override
    protected String toSetStreamName() {
        return "S32";
    }

    public final SQLStmt insertS2 = new SQLStmt(
        "INSERT INTO S33 (value) SELECT * FROM S32;"
    );
    
}
