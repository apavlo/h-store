package edu.brown.benchmark.ebay.procedures;

import org.voltdb.ProcInfo;
import org.voltdb.SQLStmt;
import org.voltdb.VoltProcedure;
import org.voltdb.VoltTable;

import edu.brown.benchmark.ebay.EbayConstants;

/**
 * GetWatchedItems
 * Description goes here...
 * @author pavlo
 * @author visawee
 */
@ProcInfo (
    partitionInfo = "USER.U_ID: 0",
    singlePartition = false
)
public class GetWatchedItems extends VoltProcedure {

    public final SQLStmt select_watched_items = new SQLStmt(
            "SELECT uw_u_id, i_id, i_u_id, i_name, i_current_price, i_end_date, i_status, uw_created " +
              "FROM " + EbayConstants.TABLENAME_USER_WATCH + ", " +
                        EbayConstants.TABLENAME_ITEM +
            " WHERE uw_u_id = ? " +
            "   AND uw_i_id = i_id AND uw_i_u_id = i_u_id " +
            " ORDER BY i_end_date ASC LIMIT 25"
        );
    
    public VoltTable[] run(long u_id) {
        voltQueueSQL(select_watched_items, u_id);
        return (voltExecuteSQL());
    }
    
}
