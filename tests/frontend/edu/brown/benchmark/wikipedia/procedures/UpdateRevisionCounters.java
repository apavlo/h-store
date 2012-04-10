package edu.brown.benchmark.wikipedia.procedures;

import org.voltdb.ProcInfo;
import org.voltdb.SQLStmt;
import org.voltdb.VoltProcedure;
import org.voltdb.VoltTable;
import org.voltdb.types.TimestampType;

import edu.brown.benchmark.wikipedia.WikipediaConstants;

@ProcInfo(
    singlePartition = false
)
public class UpdateRevisionCounters extends VoltProcedure {

    public final SQLStmt updateUser = new SQLStmt(
        "UPDATE " + WikipediaConstants.TABLENAME_USER + 
        "   SET user_editcount = ?, " +
        "       user_touched = ? " +
        " WHERE user_id = ?"
    );
    
    /**
     * The given array is a list of the user_editcount values
     * that we need to update in the database. Each index
     * in the array corresponds to the user_id
     * @param user_revision_ctr
     * @return
     */
    public VoltTable run(int user_revision_ctr[], int page_last_rev_id[], int page_last_rev_length[]) {
        final int batch_size = voltRemainingQueue();
        final TimestampType timestamp = new TimestampType();
        
        for (int i = 0; i < user_revision_ctr.length; i++) {
            voltQueueSQL(updateUser, user_revision_ctr[i],
                                     timestamp,
                                     i+1 // ids start at 1
            );
            if (i % batch_size == 0) {
                voltExecuteSQL();
            }
        }
        voltExecuteSQL(true);
        
        // VoltTable has two columns
        // #1 -> Number of users updated
        // #2 -> Number of pages updated
        
        
        // return (user_revision_ctr.length);
        return (null);
    }
    
    
}
