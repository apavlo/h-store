package edu.brown.benchmark.microexperiments.btriggers.trig4.procedures;
import org.voltdb.SQLStmt;
import org.voltdb.VoltTrigger;

import edu.brown.benchmark.microexperiments.btriggers.trig4.BTriggersConstants;

public class Trigger6 extends VoltTrigger {

    @Override
    protected String toSetStreamName() {
        return "a_str6";
    }

     // step 1: Validate contestants
    public final SQLStmt insertS1Stmt = 
        new SQLStmt("INSERT INTO a_str7 SELECT * FROM a_str6 WHERE a_val > 6;");
    
    public final SQLStmt insertATblStmt = 
            new SQLStmt("INSERT INTO a_tbl SELECT * FROM a_str6 WHERE a_val = 6;");

}
