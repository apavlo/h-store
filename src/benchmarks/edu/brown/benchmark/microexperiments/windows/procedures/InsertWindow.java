package edu.brown.benchmark.microexperiments.windows.procedures;
import org.voltdb.ProcInfo;
import org.voltdb.SQLStmt;
import org.voltdb.VoltTrigger;


@ProcInfo (
		//partitionInfo = "w_rows.phone_number:1",
	    singlePartition = true
	)
public class InsertWindow extends VoltTrigger {

    @Override
    protected String toSetStreamName() {
        return "A_STREAM";
    }

     // step 1: Validate contestants
    public final SQLStmt insertS1Stmt = 
        new SQLStmt("INSERT INTO A_WIN (a_id, a_val) SELECT * FROM A_STREAM;");

}
