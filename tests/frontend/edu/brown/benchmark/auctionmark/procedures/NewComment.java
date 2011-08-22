package edu.brown.benchmark.auctionmark.procedures;

import org.voltdb.ProcInfo;
import org.voltdb.SQLStmt;
import org.voltdb.VoltProcedure;
import org.voltdb.VoltTable;
import org.voltdb.VoltType;
import org.voltdb.VoltTable.ColumnInfo;
import org.voltdb.types.TimestampType;

import edu.brown.benchmark.auctionmark.AuctionMarkBenchmarkProfile;
import edu.brown.benchmark.auctionmark.AuctionMarkConstants;

/**
 * NewComment
 * @author visawee
 */
@ProcInfo (
    partitionInfo = "USER.U_ID: 2",
    singlePartition = true
)
public class NewComment extends VoltProcedure{
	
    // -----------------------------------------------------------------
    // STATIC MEMBERS
    // -----------------------------------------------------------------
    
    private static final ColumnInfo[] RESULT_COLS = {
        new VoltTable.ColumnInfo("ic_id", VoltType.BIGINT),
        new VoltTable.ColumnInfo("ic_i_id", VoltType.BIGINT),
        new VoltTable.ColumnInfo("ic_u_id", VoltType.BIGINT),
    };
    
    // -----------------------------------------------------------------
    // STATEMENTS
    // -----------------------------------------------------------------
    
    public final SQLStmt getMaxItemCommentId = new SQLStmt(
        "SELECT MAX(ic_id) " + 
        "  FROM " + AuctionMarkConstants.TABLENAME_ITEM_COMMENT + 
        " WHERE ic_i_id = ? AND ic_u_id = ?"
    );
	
    public final SQLStmt insertItemComment = new SQLStmt(
        "INSERT INTO " + AuctionMarkConstants.TABLENAME_ITEM_COMMENT + "(" +
        	"ic_id," +
        	"ic_i_id," +
        	"ic_u_id," +
        	"ic_buyer_id," +
        	"ic_question, " +
        	"ic_created," +
        	"ic_updated " +
        ") VALUES (?,?,?,?,?,?,?)"
    );
    
    public final SQLStmt updateUser = new SQLStmt(
        "UPDATE " + AuctionMarkConstants.TABLENAME_USER + " " +
           "SET u_comments = u_comments + 1, " +
           "    u_updated = ? " +
        " WHERE u_id = ?"
    );
	
    // -----------------------------------------------------------------
    // RUN METHOD
    // -----------------------------------------------------------------
    
    public VoltTable run(TimestampType benchmarkTimes[], long item_id, long seller_id, long buyer_id, String question) {
        final TimestampType currentTime = AuctionMarkBenchmarkProfile.getScaledTimestamp(benchmarkTimes[0], benchmarkTimes[1], new TimestampType());
    	long ic_id;
    	
        // Set comment_id
        voltQueueSQL(getMaxItemCommentId, item_id, seller_id);
        VoltTable[] results = voltExecuteSQL();
        assert (1 == results.length);
        if (0 == results[0].getRowCount()) {
            ic_id = 0;
        } else {
            boolean adv = results[0].advanceRow();
            assert(adv);
            ic_id = results[0].getLong(0) + 1;
        }

        voltQueueSQL(insertItemComment, ic_id, item_id, seller_id, buyer_id, question, currentTime, currentTime);
        voltQueueSQL(updateUser, currentTime, seller_id);
        voltExecuteSQL();

        // Return new ic_id
        VoltTable ret = new VoltTable(RESULT_COLS);
        ret.addRow(ic_id, item_id, seller_id);
        return ret;
    }	
	
}