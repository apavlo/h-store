package edu.brown.benchmark.complexnostreamtrigger.procedures;

import org.voltdb.ProcInfo;
import org.voltdb.SQLStmt;
import org.voltdb.VoltProcedure;
import org.voltdb.VoltTable;

@ProcInfo(singlePartition = true)
public class Initialize extends VoltProcedure {
    public final SQLStmt operationStmt = 
        new SQLStmt("INSERT INTO votes_by_phone_number ( phone_number, num_votes ) VALUES (?, ?);");

    public long run(int size) {
		for(int i = 0; i < size; i++)
		{
			voltQueueSQL(operationStmt, i, i);
			voltExecuteSQL();
		}
        return 0;
    }
}
