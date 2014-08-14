package edu.brown.benchmark.microexperiments.btriggers.trig10.procedures;
import org.voltdb.SQLStmt;
import org.voltdb.VoltTrigger;

import edu.brown.benchmark.microexperiments.btriggers.trig10.BTriggersConstants;

public class Trigger8 extends VoltTrigger {

    @Override
    protected String toSetStreamName() {
        return "a_str8";
    }

     // step 1: Validate contestants
    public final SQLStmt insertS1Stmt = 
        new SQLStmt("INSERT INTO a_str9 SELECT * FROM a_str8 WHERE a_val > 8;");
    
    public final SQLStmt insertATblStmt = 
            new SQLStmt("INSERT INTO a_tbl SELECT * FROM a_str8 WHERE a_val = 8;");

}
