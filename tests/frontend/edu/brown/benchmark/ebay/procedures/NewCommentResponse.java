package edu.brown.benchmark.ebay.procedures;

import org.voltdb.ProcInfo;
import org.voltdb.SQLStmt;
import org.voltdb.VoltProcedure;
import org.voltdb.VoltTable;

import edu.brown.benchmark.ebay.EbayConstants;

/**
 * NewCommentResponse
 * Description goes here...
 * @author visawee
 */
@ProcInfo (
    partitionInfo = "USER.U_ID: 2",
    singlePartition = true
)
public class NewCommentResponse extends VoltProcedure{
	
    public final SQLStmt update_comment = new SQLStmt(
            "UPDATE " + EbayConstants.TABLENAME_ITEM_COMMENT + " " +
            	"SET ic_response = ? " +
            "WHERE ic_id = ? AND ic_i_id = ? AND ic_u_id = ? "
    );

    public VoltTable[] run(long i_id, long ic_id, long seller_id, String response) {
        voltQueueSQL(update_comment, response, ic_id, i_id, seller_id);
        return (voltExecuteSQL());
    }	
}