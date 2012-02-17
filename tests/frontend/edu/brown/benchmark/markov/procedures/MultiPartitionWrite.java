package edu.brown.benchmark.markov.procedures;

import org.voltdb.SQLStmt;
import org.voltdb.VoltProcedure;
import org.voltdb.VoltTable;

import edu.brown.benchmark.markov.MarkovConstants;

public class MultiPartitionWrite extends VoltProcedure {

    public SQLStmt query = new SQLStmt("SELECT * FROM " + MarkovConstants.TABLENAME_TABLEA + " WHERE A_ID = ?");

    public VoltTable[] run(long a_id, long value) {
        voltQueueSQL(query, a_id);
        return (voltExecuteSQL());
    }
}
