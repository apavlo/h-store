package edu.brown.benchmark.microexperiments.windows.procedures;
import org.voltdb.SQLStmt;
import org.voltdb.VoltTrigger;

public class InsertWindow extends VoltTrigger {

    @Override
    protected String toSetStreamName() {
        return "A_STREAM";
    }

     // step 1: Validate contestants
    public final SQLStmt insertS1Stmt = 
        new SQLStmt("INSERT INTO A_WIN SELECT * FROM A_STREAM;");

}
