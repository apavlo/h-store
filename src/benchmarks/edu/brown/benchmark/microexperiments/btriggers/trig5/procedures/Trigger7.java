package edu.brown.benchmark.microexperiments.btriggers.trig5.procedures;
import org.voltdb.SQLStmt;
import org.voltdb.VoltTrigger;

import edu.brown.benchmark.microexperiments.btriggers.trig5.BTriggersConstants;

public class Trigger7 extends VoltTrigger {

    @Override
    protected String toSetStreamName() {
        return "a_str7";
    }

     // step 1: Validate contestants
    public final SQLStmt insertS1Stmt = 
        new SQLStmt("INSERT INTO a_str8 SELECT * FROM a_str7 WHERE a_val > 7;");
    
    public final SQLStmt insertATblStmt = 
            new SQLStmt("INSERT INTO a_tbl SELECT * FROM a_str7 WHERE a_val = 7;");

}
