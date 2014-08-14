package edu.brown.benchmark.microexperiments.btriggers.trig10.procedures;
import org.voltdb.SQLStmt;
import org.voltdb.VoltTrigger;

import edu.brown.benchmark.microexperiments.btriggers.trig10.BTriggersConstants;

public class Trigger9 extends VoltTrigger {

    @Override
    protected String toSetStreamName() {
        return "a_str9";
    }

     // step 1: Validate contestants
    public final SQLStmt insertS1Stmt = 
        new SQLStmt("INSERT INTO a_str10 SELECT * FROM a_str9 WHERE a_val > 9;");
    
    public final SQLStmt insertATblStmt = 
            new SQLStmt("INSERT INTO a_tbl SELECT * FROM a_str9 WHERE a_val = 9;");

}
