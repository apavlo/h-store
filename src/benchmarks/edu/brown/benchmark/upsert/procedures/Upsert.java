package edu.brown.benchmark.upsert.procedures;

import org.voltdb.SQLStmt;
import org.voltdb.VoltProcedure;
import org.voltdb.VoltTable;
import org.voltdb.StmtInfo;

public class Upsert extends VoltProcedure {
	@StmtInfo(
        upsertable=true
    )
    public final SQLStmt operationStmt = 
        new SQLStmt("INSERT INTO votes_by_phone_number ( phone_number, num_votes ) VALUES (?, ?);");



    public long run(int size, int value) {
		for(int i = 0; i < size; i++)
		{
			voltQueueSQL(operationStmt, i, value*i);
			voltExecuteSQL();
		}
        return 0;
    }
}
