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
public class GetComment extends VoltProcedure{
	
    public final SQLStmt select_comment = new SQLStmt(
            "SELECT * FROM " + AuctionMarkConstants.TABLENAME_ITEM_COMMENT + " " + 
            "WHERE ic_u_id = ? AND ic_response = ?"
        );
	
    public VoltTable run(long seller_id) {
    	
    	// Set comment_id
    	voltQueueSQL(select_comment, seller_id, null);
    	VoltTable[] results = voltExecuteSQL();
        return results[0];
    }	
	
}