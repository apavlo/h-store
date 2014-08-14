package edu.brown.benchmark.microexperiments.btriggers.trig7.procedures;
import org.voltdb.SQLStmt;
import org.voltdb.VoltTrigger;

import edu.brown.benchmark.microexperiments.btriggers.trig7.BTriggersConstants;

public class Trigger10 extends VoltTrigger {

    @Override
    protected String toSetStreamName() {
        return "a_str10";
    }

     // step 1: Validate contestants
    public final SQLStmt insertATblStmt = 
            new SQLStmt("INSERT INTO a_tbl SELECT * FROM a_str10 WHERE a_val = 10;");

}
