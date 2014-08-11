package edu.brown.benchmark.microexperiments.btriggers.procedures;
import org.voltdb.SQLStmt;
import org.voltdb.VoltTrigger;

public class BackendTrigger extends VoltTrigger {

    @Override
    protected String toSetStreamName() {
        return "a_stream";
    }

     // step 1: Validate contestants
    public final SQLStmt insertS1Stmt = 
        new SQLStmt("INSERT INTO a_tbl SELECT * FROM a_stream;");

}
