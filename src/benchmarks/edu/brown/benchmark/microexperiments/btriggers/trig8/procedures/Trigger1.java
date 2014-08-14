package edu.brown.benchmark.microexperiments.btriggers.trig8.procedures;
import org.voltdb.SQLStmt;
import org.voltdb.VoltTrigger;

import edu.brown.benchmark.microexperiments.btriggers.trig8.BTriggersConstants;

public class Trigger1 extends VoltTrigger {

    @Override
    protected String toSetStreamName() {
        return "a_str1";
    }

     // step 1: Validate contestants
    public final SQLStmt insertS1Stmt = 
        new SQLStmt("INSERT INTO a_str2 SELECT * FROM a_str1 WHERE a_val > 1;");

    public final SQLStmt insertATblStmt = 
        new SQLStmt("INSERT INTO a_tbl SELECT * FROM a_str1 WHERE a_val = 1;");
}
