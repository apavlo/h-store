package edu.brown.benchmark.auctionmark.procedures;

import java.util.Random;

import org.voltdb.ProcInfo;
import org.voltdb.SQLStmt;
import org.voltdb.VoltProcedure;
import org.voltdb.VoltTable;
import org.voltdb.VoltType;
import org.voltdb.types.TimestampType;

import edu.brown.benchmark.auctionmark.AuctionMarkConstants;
import edu.brown.benchmark.auctionmark.util.ItemId;

/**
 * NewPurchase
 * Description goes here...
 * @author visawee
 */
@ProcInfo (
    singlePartition = true
)
public class NewPurchase extends VoltProcedure{
	
    // -----------------------------------------------------------------
    // STATIC MEMBERS
    // -----------------------------------------------------------------
    
    private static final VoltTable.ColumnInfo[] RESULT_COLS = {
        new VoltTable.ColumnInfo("ip_id", VoltType.BIGINT),
        new VoltTable.ColumnInfo("ip_ib_id", VoltType.BIGINT), 
        new VoltTable.ColumnInfo("ip_ib_i_id", VoltType.BIGINT),
        new VoltTable.ColumnInfo("u_id", VoltType.BIGINT),
        new VoltTable.ColumnInfo("ip_ib_u_id", VoltType.BIGINT)
    };
    
    // -----------------------------------------------------------------
    // STATEMENTS
    // -----------------------------------------------------------------
    
    public final SQLStmt selectBuyerInfo = new SQLStmt(
        "SELECT imb_ib_id, imb_ib_i_id, imb_ib_u_id, " +
        "       ib_id, ib_buyer_id, ib_bid, ib_max_bid, " +
        "       u_balance " +
		"  FROM " + AuctionMarkConstants.TABLENAME_ITEM_MAX_BID + ", " +
		            AuctionMarkConstants.TABLENAME_ITEM_BID + ", " +
		            AuctionMarkConstants.TABLENAME_USER +
        " WHERE imb_i_id = ? AND imb_u_id = ? " +
        "   AND imb_ib_id = ib_id AND imb_ib_i_id = ib_i_id AND imb_ib_u_id = ib_u_id " +
        "   AND ib_buyer_id = u_id "
    );

    public final SQLStmt insertPurchase = new SQLStmt(
        "INSERT INTO " + AuctionMarkConstants.TABLENAME_ITEM_PURCHASE + "(" +
        	"ip_id," +
        	"ip_ib_id," +
        	"ip_ib_i_id," +  
        	"ip_ib_u_id," +  
        	"ip_date" +     
        ") VALUES(?,?,?,?,?)"
    );
    
    public final SQLStmt updateItemStatus = new SQLStmt(
        "UPDATE " + AuctionMarkConstants.TABLENAME_ITEM + " " +
        	"SET i_status = " + AuctionMarkConstants.STATUS_ITEM_CLOSED + " " +   
        "WHERE i_id = ? AND i_u_id = ? "
    );    
    
    public final SQLStmt updateUserItem = new SQLStmt(
        "UPDATE " + AuctionMarkConstants.TABLENAME_USER_ITEM + " " +
           "SET ui_ip_id = ?, " +
           "    ui_ip_ib_id = ?, " +
           "    ui_ip_ib_i_id = ?, " +
           "    ui_ip_ib_u_id = ?" +
        " WHERE ui_u_id = ? AND ui_i_id = ? AND ui_i_u_id = ?"
    );
    
    public final SQLStmt updateUserBalance = new SQLStmt(
        "UPDATE " + AuctionMarkConstants.TABLENAME_USER + " " +
           "SET u_balance = u_balance + ? " + 
        " WHERE u_id = ?"
    );
    
    // -----------------------------------------------------------------
    // RUN METHOD
    // -----------------------------------------------------------------
    
    public VoltTable run(long item_id, long seller_id, double buyer_credit) throws VoltAbortException {
        // Get the ITEM_MAX_BID record so that we know what we need to process
        voltQueueSQL(selectBuyerInfo, item_id, seller_id);
        VoltTable results[] = voltExecuteSQL();
        assert (results.length == 1);
        if (results[0].getRowCount() == 0) {
            throw new VoltAbortException("No ITEM_MAX_BID record for item " + item_id);
        }
        assert (results[0].getRowCount() == 1);
        boolean adv = results[0].advanceRow();
        assert (adv);
//        long imb_ib_id = results[0].getLong(0);
//        long imb_ib_i_id = results[0].getLong(1);
//        long imb_ib_u_id = results[0].getLong(2);
//        long ib_id = results[0].getLong(3);
        long ib_buyer_id = results[0].getLong(4);
        double ib_bid = results[0].getDouble(5);
        double ib_max_bid = results[0].getDouble(6);
        double u_balance = results[0].getDouble(7);
        
        // Make sure that the buyer has enough money to cover this charge
        // We can add in a credit for the buyer's account
        if (ib_max_bid > (buyer_credit + u_balance)) {
            throw new VoltAbortException(String.format("Buyer does not have enough money in account to purchase item " +
                                                       "[maxBid=%.2f, balance=%.2f, credit=%.2f]",
                                                       ib_max_bid, u_balance, buyer_credit));
        }

        // Set item_purchase_id
        long ip_id = ItemId.getUniqueElementId(item_id, 1);

        // Insert a new purchase
        // System.err.println(String.format("NewPurchase: ip_id=%d, ib_bid=%.2f, item_id=%d, seller_id=%d", ip_id, ib_bid, item_id, seller_id));
        voltQueueSQL(insertPurchase, ip_id, ib_bid, item_id, seller_id, new TimestampType());
        
        // Update item status to close
        voltQueueSQL(updateItemStatus, item_id, seller_id);
        
        // And update this the USER_ITEM record to link it to the new ITEM_PURCHASE record
        voltQueueSQL(updateUserItem, ip_id, ib_bid, item_id, seller_id, ib_buyer_id, item_id, seller_id);
        
        // Decrement the buyer's account and credit the seller's account
        voltQueueSQL(updateUserBalance, -1*(ib_max_bid) + buyer_credit, ib_buyer_id);
        voltQueueSQL(updateUserBalance, -ib_max_bid, seller_id);
        
        results = voltExecuteSQL();
        assert(results.length > 0);

        // Return ip_id, ip_ib_id, ip_ib_i_id, u_id, ip_ib_u_id
        VoltTable ret = new VoltTable(RESULT_COLS);
        ret.addRow(new Object[] { ip_id, ib_bid, item_id, seller_id, ib_buyer_id });
        
        return ret;
    }	
}