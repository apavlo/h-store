package edu.brown.benchmark.microexperiments.btriggers.trig7.procedures;
import org.voltdb.SQLStmt;
import org.voltdb.VoltTrigger;

import edu.brown.benchmark.microexperiments.btriggers.trig7.BTriggersConstants;

public class Trigger4 extends VoltTrigger {

    @Override
    protected String toSetStreamName() {
        return "a_str4";
    }

     // step 1: Validate contestants
    public final SQLStmt insertS1Stmt = 
        new SQLStmt("INSERT INTO a_str5 SELECT * FROM a_str4 WHERE a_val > 4;");

    public final SQLStmt insertATblStmt = 
            new SQLStmt("INSERT INTO a_tbl SELECT * FROM a_str4 WHERE a_val = 4;");
}
