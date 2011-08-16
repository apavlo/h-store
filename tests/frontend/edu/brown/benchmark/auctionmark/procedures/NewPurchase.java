package edu.brown.benchmark.auctionmark.procedures;

import org.voltdb.ProcInfo;
import org.voltdb.SQLStmt;
import org.voltdb.VoltProcedure;
import org.voltdb.VoltTable;
import org.voltdb.VoltType;
import org.voltdb.types.TimestampType;

import edu.brown.benchmark.auctionmark.AuctionMarkConstants;

/**
 * NewPurchase
 * Description goes here...
 * @author visawee
 */
@ProcInfo (
    partitionInfo = "USER.U_ID: 2",
    singlePartition = true
)
public class NewPurchase extends VoltProcedure{
	
    private static final VoltTable.ColumnInfo[] RESULT_COLS = new VoltTable.ColumnInfo[] {
        new VoltTable.ColumnInfo("ip_id", VoltType.BIGINT),
        new VoltTable.ColumnInfo("ip_ib_id", VoltType.BIGINT), 
        new VoltTable.ColumnInfo("ip_ib_i_id", VoltType.BIGINT),
        new VoltTable.ColumnInfo("u_id", VoltType.BIGINT),
        new VoltTable.ColumnInfo("ip_ib_u_id", VoltType.BIGINT)
    };
    
    public final SQLStmt insert_purchase = new SQLStmt(
        "INSERT INTO " + AuctionMarkConstants.TABLENAME_ITEM_PURCHASE + "(" +
        	"ip_id," +
        	"ip_ib_id," +
        	"ip_ib_i_id," +  
        	"ip_ib_u_id," +  
        	"ip_date" +     
        ") VALUES(?,?,?,?,?)"
    );
    
    public final SQLStmt update_item_status = new SQLStmt(
        "UPDATE " + AuctionMarkConstants.TABLENAME_ITEM + " " +
        	"SET i_status = " + AuctionMarkConstants.STATUS_ITEM_CLOSED + " " +   
        "WHERE i_id = ? AND i_u_id = ? "
    );    
    
    public final SQLStmt update_user_item = new SQLStmt(
        "UPDATE " + AuctionMarkConstants.TABLENAME_USER_ITEM + " " +
           "SET ui_ip_id = ?, " +
           "    ui_ip_ib_id = ?, " +
           "    ui_ip_ib_i_id = ?, " +
           "    ui_ip_ib_u_id = ?" +
        " WHERE ui_u_id = ? AND ui_i_id = ? AND ui_i_u_id = ?"
    );
    
    public final SQLStmt select_item_max_bid = new SQLStmt(
        "SELECT imb_ib_id, imb_ib_i_id, imb_ib_u_id, " +
        "       ib_id, ib_buyer_id, ib_bid, ib_max_bid " +
		"  FROM " + AuctionMarkConstants.TABLENAME_ITEM_MAX_BID + ", " +
		            AuctionMarkConstants.TABLENAME_ITEM_BID +
        " WHERE imb_i_id = ? AND imb_u_id = ? " +
        "   AND imb_ib_id = ib_id AND imb_ib_i_id = ib_i_id AND imb_ib_u_id = ib_u_id "
    );
    
    public final SQLStmt select_max_purchase_id = new SQLStmt(
        "SELECT MAX(ip_id) " + 
		"FROM " + AuctionMarkConstants.TABLENAME_ITEM_PURCHASE + " " +
        "WHERE ip_ib_id = ? AND ip_ib_i_id = ? AND ip_ib_u_id = ?"
    );    
    
    public VoltTable run(long item_id, long seller_id) throws VoltAbortException {
        // Get the ITEM_MAX_BID record so that we know what we need to process
        voltQueueSQL(select_item_max_bid, item_id, seller_id);
        VoltTable results[] = voltExecuteSQL();
        assert (results.length == 1);
        if (results[0].getRowCount() == 0) {
            throw new VoltAbortException("No ITEM_MAX_BID record for item " + item_id);
        }
        assert (results[0].getRowCount() == 1);
        boolean adv = results[0].advanceRow();
        assert (adv);
        long imb_ib_id = results[0].getLong(0);
        long imb_ib_i_id = results[0].getLong(1);
        long imb_ib_u_id = results[0].getLong(2);
        long ib_id = results[0].getLong(3);
        long ib_buyer_id = results[0].getLong(4);
        double ib_bid = results[0].getDouble(5);
        double ib_max_bid = results[0].getDouble(6);

        // Set item_purchase_id
        voltQueueSQL(select_max_purchase_id, ib_bid, item_id, seller_id);
        results = voltExecuteSQL();
        assert (results.length == 1);
        long ip_id = 0;
        while (results[0].advanceRow()) {
            long temp = results[0].getLong(0);
            ip_id = (results[0].wasNull() ? 0 : temp + 1);
        } // WHILE

        // Insert a new purchase
        voltQueueSQL(insert_purchase, ip_id, ib_bid, item_id, seller_id, new TimestampType());
        
        // Update item status to close
        voltQueueSQL(update_item_status, item_id, seller_id);
        
        // And update this the USER_ITEM record to link it to the new ITEM_PURCHASE record
        voltQueueSQL(update_user_item, ip_id, ib_bid, item_id, seller_id, ib_buyer_id, item_id, seller_id);
        
        results = voltExecuteSQL();
        assert(results.length > 0);

        // Return ip_id, ip_ib_id, ip_ib_i_id, u_id, ip_ib_u_id
        VoltTable ret = new VoltTable(RESULT_COLS);
        ret.addRow(new Object[] { ip_id, ib_bid, item_id, seller_id, ib_buyer_id });
        
        return ret;
    }	
}