package edu.brown.benchmark.auctionmark.procedures;

import org.voltdb.ProcInfo;
import org.voltdb.SQLStmt;
import org.voltdb.VoltProcedure;
import org.voltdb.VoltTable;
import org.voltdb.types.TimestampType;

import edu.brown.benchmark.auctionmark.AuctionMarkProfile;
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
        	"SET ic_response = ?, " +
        	"    ic_updated = ? " +
        "WHERE ic_id = ? AND ic_i_id = ? AND ic_u_id = ? "
    );
    
    public final SQLStmt updateUser = new SQLStmt(
        "UPDATE " + AuctionMarkConstants.TABLENAME_USER + " " +
           "SET u_comments = u_comments - 1, " +
           "    u_updated = ? " +
        " WHERE u_id = ?"
    );

    // -----------------------------------------------------------------
    // RUN METHOD
    // -----------------------------------------------------------------
    
    public VoltTable[] run(TimestampType benchmarkTimes[], long item_id, long seller_id, long comment_id, String response) {
        final TimestampType currentTime = AuctionMarkProfile.getScaledTimestamp(benchmarkTimes[0], benchmarkTimes[1], new TimestampType());
        voltQueueSQL(updateComment, response, currentTime, comment_id, item_id, seller_id);
        voltQueueSQL(updateUser, currentTime, seller_id);
        return (voltExecuteSQL());
    }	
}