package edu.brown.benchmark.microexperiments.btriggers.trig9.procedures;
import org.voltdb.SQLStmt;
import org.voltdb.VoltTrigger;

import edu.brown.benchmark.microexperiments.btriggers.trig9.BTriggersConstants;

public class Trigger5 extends VoltTrigger {

    @Override
    protected String toSetStreamName() {
        return "a_str5";
    }

     // step 1: Validate contestants
    public final SQLStmt insertS1Stmt = 
        new SQLStmt("INSERT INTO a_str6 SELECT * FROM a_str5 WHERE a_val > 5;");
    
    public final SQLStmt insertATblStmt = 
            new SQLStmt("INSERT INTO a_tbl SELECT * FROM a_str5 WHERE a_val = 5;");

}
