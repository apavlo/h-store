package edu.brown.benchmark.simplestreamtrigger.procedures;

import org.voltdb.SQLStmt;
import org.voltdb.StmtInfo;
import org.voltdb.VoltTrigger;

public class SimpleTrigger extends VoltTrigger {

    @Override
    protected String toSetStreamName() {
        return "S1";
    }

//    public final SQLStmt simpleSelect = new SQLStmt(
//        "SELECT * from S1;"
//    );
    
}
