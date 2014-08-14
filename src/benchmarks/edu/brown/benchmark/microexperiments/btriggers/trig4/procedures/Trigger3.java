package edu.brown.benchmark.microexperiments.btriggers.trig4.procedures;
import org.voltdb.SQLStmt;
import org.voltdb.VoltTrigger;

import edu.brown.benchmark.microexperiments.btriggers.trig4.BTriggersConstants;

public class Trigger3 extends VoltTrigger {

    @Override
    protected String toSetStreamName() {
        return "a_str3";
    }

     // step 1: Validate contestants
    public final SQLStmt insertS1Stmt = 
        new SQLStmt("INSERT INTO a_str4 SELECT * FROM a_str3 WHERE a_val > 3;");
    
    public final SQLStmt insertATblStmt = 
            new SQLStmt("INSERT INTO a_tbl SELECT * FROM a_str3 WHERE a_val = 3;");

}
