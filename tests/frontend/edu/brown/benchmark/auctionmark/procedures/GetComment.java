package edu.brown.benchmark.auctionmark.procedures;

import org.voltdb.ProcInfo;
import org.voltdb.SQLStmt;
import org.voltdb.VoltProcedure;
import org.voltdb.VoltTable;

import edu.brown.benchmark.auctionmark.AuctionMarkConstants;

/**
 * GetComment
 * Description goes here
 * @author visawee
 */
@ProcInfo (
    partitionInfo = "USER.U_ID: 0",
    singlePartition = true
)
public class GetComment extends VoltProcedure {
	
    public final SQLStmt select_comments = new SQLStmt(
        "SELECT " + AuctionMarkConstants.TABLENAME_ITEM_COMMENT + ".*,  " +
        "       i_name, i_current_price, i_num_bids, i_end_date " +
        "  FROM " + AuctionMarkConstants.TABLENAME_ITEM + ", " + 
                    AuctionMarkConstants.TABLENAME_ITEM_COMMENT +
        " WHERE i_id = ? AND i_u_id = ? AND i_status = " + AuctionMarkConstants.STATUS_ITEM_OPEN + 
        "   AND i_id = ic_i_id AND i_u_id = ic_u_id AND ic_response = ?"
    );
	
    public VoltTable[] run(long item_id, long seller_id) {
    	voltQueueSQL(select_comments, item_id, seller_id, null);
    	return (voltExecuteSQL());
    }	
	
}