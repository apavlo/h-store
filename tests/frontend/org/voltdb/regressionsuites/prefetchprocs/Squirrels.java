package org.voltdb.regressionsuites.prefetchprocs;

import org.voltdb.SQLStmt;
import org.voltdb.VoltProcedure;
import org.voltdb.VoltTable;

import edu.brown.hstore.dtxn.LocalTransaction;
import edu.brown.utils.ThreadUtil;

public class Squirrels extends VoltProcedure {
    
    public final SQLStmt getLocal = new SQLStmt(
        "SELECT * FROM TABLEA WHERE A_ID = ?"
    );
    
    public final SQLStmt getRemote = new SQLStmt(
        "SELECT SUM(B_VALUE) FROM TABLEB WHERE B_A_ID = ?"
    );
    
    public final SQLStmt updateLocal = new SQLStmt(
        "UPDATE TABLEA SET A_VALUE = ? WHERE A_ID = ?"
    );
    
    public VoltTable[] run(long a_id, long sleep) {
        voltQueueSQL(getLocal, a_id);
        final VoltTable a_results[] = voltExecuteSQL();
        assert(a_results.length == 1);
        
        // Force a delay if we're a distributed transaction
        if (((LocalTransaction)this.getTransactionState()).isPredictSinglePartition() == false) {
            System.err.printf("Sleeping for %.01f seconds\n", sleep / 1000d);
            ThreadUtil.sleep(sleep);
        }
        
        voltQueueSQL(getRemote, a_id);
        final VoltTable b_results[] = voltExecuteSQL();
        assert(b_results.length == 1);
        long sum = b_results[0].asScalarLong();
        
        voltQueueSQL(updateLocal, sum, a_id);
        return (voltExecuteSQL(true));
    }

}
