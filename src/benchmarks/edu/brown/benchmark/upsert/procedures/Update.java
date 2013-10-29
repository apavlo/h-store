package edu.brown.benchmark.upsert.procedures;

import org.voltdb.SQLStmt;
import org.voltdb.VoltProcedure;
import org.voltdb.VoltTable;

public class Update extends VoltProcedure {
    public final SQLStmt operationStmt = new SQLStmt(
		"UPDATE votes_by_phone_number " +
		"SET num_votes = ?" +
		" WHERE phone_number = ?"
		);

    public long run(int size) {
		for(int i = 0; i < size; i++)
		{
			voltQueueSQL(operationStmt, 100*i, i);
			voltExecuteSQL();
		}
        return 0;
    }
}
