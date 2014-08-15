package edu.brown.benchmark.microexperiments.windows.procedures;
import org.voltdb.ProcInfo;
import org.voltdb.SQLStmt;
import org.voltdb.VoltTrigger;

@ProcInfo (
		//partitionInfo = "w_rows.phone_number:1",
	    singlePartition = true
	)
public class BackendTrigger extends VoltTrigger {

    @Override
    protected String toSetStreamName() {
        return "A_WIN";
    }

     // step 1: Validate contestants
    public final SQLStmt insertS1Stmt = 
        new SQLStmt("INSERT INTO a_tbl (a_id, a_val) SELECT min(a_id), max(a_val) FROM A_WIN;");

}
