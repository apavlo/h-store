package edu.brown.benchmark.auctionmark.procedures;

import org.voltdb.ProcInfo;
import org.voltdb.SQLStmt;
import org.voltdb.VoltProcedure;
import org.voltdb.VoltTable;
import org.voltdb.types.TimestampType;

import edu.brown.benchmark.auctionmark.AuctionMarkConstants;
import edu.brown.benchmark.auctionmark.AuctionMarkConstants.ItemStatus;

/**
 * Get Item Information
 * Returns all of the attributes for a particular item
 * @author pavlo
 */
@ProcInfo (
    partitionInfo = "USER.U_ID: 2",
    singlePartition = true
)
public class GetItem extends VoltProcedure {

    // -----------------------------------------------------------------
    // STATEMENTS
    // -----------------------------------------------------------------
    
    public final SQLStmt getItem = new SQLStmt(
        "SELECT i_id, i_u_id, i_initial_price, i_current_price, i_num_bids, i_end_date, i_status " +
          "FROM " + AuctionMarkConstants.TABLENAME_ITEM + " " + 
         "WHERE i_id = ? AND i_u_id = ? AND i_status = " + ItemStatus.OPEN.ordinal()
    );
    
    public final SQLStmt getUser = new SQLStmt(
        "SELECT u_id, u_rating, u_created, u_sattr0, u_sattr1, u_sattr2, u_sattr3, u_sattr4, r_name " +
        "  FROM " + AuctionMarkConstants.TABLENAME_USER + ", " +
                    AuctionMarkConstants.TABLENAME_REGION +
        " WHERE u_id = ? AND u_r_id = r_id"
    );

    // -----------------------------------------------------------------
    // RUN METHOD
    // -----------------------------------------------------------------
    
    public VoltTable[] run(TimestampType benchmarkTimes[], long item_id, long seller_id) {
        voltQueueSQL(getItem, item_id, seller_id);
        voltQueueSQL(getUser, seller_id);
        return (voltExecuteSQL(true));
    }
    
}
