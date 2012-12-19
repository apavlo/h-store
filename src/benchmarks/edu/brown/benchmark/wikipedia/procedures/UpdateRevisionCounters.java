package edu.brown.benchmark.wikipedia.procedures;

import org.voltdb.SQLStmt;
import org.voltdb.VoltProcedure;
import org.voltdb.VoltTable;
import org.voltdb.types.TimestampType;

import edu.brown.benchmark.wikipedia.WikipediaConstants;

/**
 * Bulk update the revision counters for users/pages
 * @author xin
 * @author pavlo
 */
public class UpdateRevisionCounters extends VoltProcedure {
    
    public final SQLStmt updateUser = new SQLStmt(
            "UPDATE " + WikipediaConstants.TABLENAME_USER + 
            "   SET user_editcount = ?, " +
            "       user_touched = ? " +
            " WHERE user_id = ?"
            );

    public final SQLStmt updatePage = new SQLStmt (
            "UPDATE " + WikipediaConstants.TABLENAME_PAGE + 
            "   SET page_latest = ?, " +
            "       page_touched = ?, " +
            "       page_is_new = 0, " +
            "       page_is_redirect = 0, " +
            "       page_len = ? " +
            " WHERE page_id = ?"
            );
        
    /**
     * The given array is a list of the user_editcount values that we need to
     * update in the database. Each index in the array corresponds to the
     * user_id
     * 
     * @param user_revision_ctr
     * @return
     */
    public VoltTable run(int user_revision_ctr[], int num_pages, int page_last_rev_id[], int page_last_rev_length[]) {
        final TimestampType timestamp = new TimestampType();
        final int batch_size = voltRemainingQueue();
        int ctr = 0;
        for (int i = 0; i < user_revision_ctr.length; i++) {
            voltQueueSQL(updateUser, user_revision_ctr[i], 
                    timestamp, 
                    i + 1 // ids start at 1
            );
            ctr++;
            if (i % batch_size == 0) {
                voltExecuteSQL();
                ctr = 0;
            }
        }
        if (ctr > 0) voltExecuteSQL();
        
        ctr = 0;
        for (int i = 0; i < num_pages; i++) {
            if (page_last_rev_id[i] == -1) continue;
            voltQueueSQL(updatePage, page_last_rev_id[i], 
                    timestamp, 
                    page_last_rev_length[i], 
                    i + 1
            );
            ctr++;
            if (i % batch_size == 0) {
                voltExecuteSQL();
                ctr = 0;
            }
        } // FOR
        if (ctr > 0) voltExecuteSQL();

        // VoltTable has two columns
        // #1 -> Number of users updated
        // #2 -> Number of pages updated

        VoltTable result = new VoltTable(WikipediaConstants.GET_USER_PAGE_UPDATE_COLS);
        
        result.addRow(user_revision_ctr.length, page_last_rev_length.length);
        
        return (result);
    }

}
