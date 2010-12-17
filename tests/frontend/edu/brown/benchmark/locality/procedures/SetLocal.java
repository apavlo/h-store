package edu.brown.benchmark.locality.procedures;

import org.voltdb.SQLStmt;
import org.voltdb.VoltProcedure;
import org.voltdb.VoltTable;

import edu.brown.benchmark.markov.MarkovConstants;

public class SetLocal extends VoltProcedure {

    public final SQLStmt writeA = new SQLStmt(
    		"UPDATE TABLEA SET A_VALUE = ? WHERE A_ID = ?");
    
    public final SQLStmt writeB = new SQLStmt(
	"UPDATE TABLEB SET B_ID = ?, B_VALUE = ? WHERE B_A_ID in ?");

    /**
     * 
     * @param a_id
     * @param a_value
     * @param b_id
     * @param b_value
     * @return
     */
    public VoltTable[] run(long a_id, String a_value, long b_id, String b_value) {
        voltQueueSQL(writeA, a_value, a_id);
        // the relation is multiply a_id by 1000
        voltQueueSQL(writeB, a_id * 1000 + b_id, b_value, a_id);
        VoltTable[] result = voltExecuteSQL();
        if (result[0].fetchRow(0).getLong(0) == 1 && result[1].fetchRow(0).getLong(0) == 1)
        {
            // query executed properly
            return voltExecuteSQL();      	
        }
        else
        {
        	// one of the queries failed to run
        	return null;
        }
    }
    
}
