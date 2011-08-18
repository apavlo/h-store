package edu.brown.benchmark.auctionmark.procedures;

import org.voltdb.ProcInfo;
import org.voltdb.SQLStmt;
import org.voltdb.VoltProcedure;
import org.voltdb.VoltTable;

import edu.brown.benchmark.auctionmark.AuctionMarkConstants;

/**
 * NewCommentResponse
 * @author pavlo
 * @author visawee
 */
@ProcInfo (
    partitionInfo = "USER.U_ID: 2",
    singlePartition = true
)
public class NewCommentResponse extends VoltProcedure{
	
    // -----------------------------------------------------------------
    // STATEMENTS
    // -----------------------------------------------------------------
    
    public final SQLStmt updateComment = new SQLStmt(
        "UPDATE " + AuctionMarkConstants.TABLENAME_ITEM_COMMENT + " " +
        	"SET ic_response = ? " +
        "WHERE ic_id = ? AND ic_i_id = ? AND ic_u_id = ? "
    );
    
    public final SQLStmt updateUser = new SQLStmt(
        "UPDATE " + AuctionMarkConstants.TABLENAME_USER + " " +
           "SET u_comments = u_comments - 1 " + 
        " WHERE u_id = ?"
    );

    // -----------------------------------------------------------------
    // RUN METHOD
    // -----------------------------------------------------------------
    
    public VoltTable[] run(long i_id, long ic_id, long seller_id, String response) {
        voltQueueSQL(updateComment, response, ic_id, i_id, seller_id);
        voltQueueSQL(updateUser, seller_id);
        return (voltExecuteSQL());
    }	
}