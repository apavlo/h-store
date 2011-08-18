package edu.brown.benchmark.auctionmark.procedures;

import org.voltdb.ProcInfo;
import org.voltdb.SQLStmt;
import org.voltdb.VoltProcedure;
import org.voltdb.VoltTable;

import edu.brown.benchmark.auctionmark.AuctionMarkConstants;

/**
 * Get Item Information
 * Returns all of the attributes for a particular item
 * @author pavlo
 */
@ProcInfo (
    partitionInfo = "USER.U_ID: 1"
)
public class GetItem extends VoltProcedure {

    // -----------------------------------------------------------------
    // STATEMENTS
    // -----------------------------------------------------------------
    
    public final SQLStmt selectItem = new SQLStmt(
        "SELECT i_id, i_u_id, i_initial_price, i_current_price " +
          "FROM " + AuctionMarkConstants.TABLENAME_ITEM + " " + 
         "WHERE i_id = ? AND i_u_id = ? AND i_status = " + AuctionMarkConstants.STATUS_ITEM_OPEN
    );
    
    public final SQLStmt selectUser = new SQLStmt(
        "SELECT u_id, u_rating, u_created, u_sattr0, u_sattr1, u_sattr2, u_sattr3, u_sattr4, r_name " +
        "  FROM " + AuctionMarkConstants.TABLENAME_USER + ", " +
                    AuctionMarkConstants.TABLENAME_REGION +
        " WHERE u_id = ? AND u_r_id = r_id"
    );

    // -----------------------------------------------------------------
    // RUN METHOD
    // -----------------------------------------------------------------
    
    public VoltTable[] run(long item_id, long seller_id) {
        voltQueueSQL(selectItem, item_id, seller_id);
        voltQueueSQL(selectUser, seller_id);
        return (voltExecuteSQL(true));
    }
    
}
