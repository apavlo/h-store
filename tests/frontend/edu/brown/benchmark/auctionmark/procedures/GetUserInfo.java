package edu.brown.benchmark.auctionmark.procedures;

import org.voltdb.ProcInfo;
import org.voltdb.SQLStmt;
import org.voltdb.VoltProcedure;
import org.voltdb.VoltTable;

import edu.brown.benchmark.auctionmark.AuctionMarkConstants;

/**
 * GetUserInfo
 * Description goes here...
 * @author pavlo
 * @author visawee
 */
@ProcInfo (
    partitionInfo = "USER.U_ID: 0",
    singlePartition = false
)
public class GetUserInfo extends VoltProcedure {
    
    public final SQLStmt select_user = new SQLStmt(
            "SELECT u_id, u_rating, u_created, u_sattr0, u_sattr1, u_sattr2, u_sattr3, u_sattr4, r_name " +
            "  FROM " + AuctionMarkConstants.TABLENAME_USER + ", " +
                        AuctionMarkConstants.TABLENAME_REGION +
            " WHERE u_id = ? AND u_r_id = r_id"
        );

    public final SQLStmt select_seller_items = new SQLStmt(
            "SELECT i_id, i_u_id, i_name, i_current_price, i_end_date, i_status " +
              "FROM " + AuctionMarkConstants.TABLENAME_ITEM +
            " WHERE i_u_id = ? ORDER BY i_end_date ASC "
        );

    public final SQLStmt select_seller_feedback = new SQLStmt(
            "SELECT if_rating, if_comment, if_date, " +
                   "i_id, i_u_id, i_name, i_end_date, i_status, "+
                   "u_id, u_rating, u_sattr0, u_sattr1 " +
              "FROM " + AuctionMarkConstants.TABLENAME_ITEM_FEEDBACK + ", " +
                        AuctionMarkConstants.TABLENAME_ITEM + ", " +
                        AuctionMarkConstants.TABLENAME_USER +
            " WHERE if_u_id = ? AND if_i_id = i_id AND if_u_id = i_u_id " +
               "AND if_buyer_id = u_id " +
            " ORDER BY if_date DESC "
        );

    
    public VoltTable[] run(long u_id, long get_items, long get_feedback) {
        voltQueueSQL(select_user, u_id);
        final VoltTable user_results[] = voltExecuteSQL();
        assert(user_results.length == 1);
        
        // Get Seller's Items (33% of the time)
        if (get_items == 1) {
            voltQueueSQL(select_seller_items, u_id);            
            
         // Get Seller's Feeback (33% of the time)
        } else if (get_feedback == 1) {
            voltQueueSQL(select_seller_feedback, u_id);
        }
        
        // Important: You have to make sure that none of the entries in the final
        //            VoltTable results array that get passed back are null, otherwise
        //            the ExecutionSite will throw an error!
        VoltTable results[] = null;
        if (get_items == 1 || get_feedback == 1) {
            VoltTable extra_results[] = voltExecuteSQL();
            assert(extra_results.length == 1);
            results = new VoltTable[] { user_results[0], extra_results[0] };
        } else {
            results = new VoltTable[] { user_results[0] };
        }
        return (results);
    }
    
}
