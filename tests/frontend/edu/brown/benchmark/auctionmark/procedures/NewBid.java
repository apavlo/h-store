package edu.brown.benchmark.auctionmark.procedures;

import org.apache.log4j.Logger;
import org.voltdb.ProcInfo;
import org.voltdb.SQLStmt;
import org.voltdb.VoltProcedure;
import org.voltdb.VoltTable;
import org.voltdb.VoltType;
import org.voltdb.types.TimestampType;

import edu.brown.benchmark.auctionmark.AuctionMarkConstants;
import edu.brown.benchmark.auctionmark.util.ItemId;
import edu.brown.benchmark.auctionmark.util.UserId;

/**
 * NewBid
 * @author pavlo
 * @author visawee
 */
@ProcInfo (
    partitionInfo = "USER.U_ID: 2",
    singlePartition = true
)
public class NewBid extends VoltProcedure {
    private static final Logger LOG = Logger.getLogger(CloseAuctions.class);

    // -----------------------------------------------------------------
    // STATIC MEMBERS
    // -----------------------------------------------------------------
    
    private static final VoltTable.ColumnInfo[] RESULT_COLS = {
        new VoltTable.ColumnInfo("i_id", VoltType.BIGINT),
        new VoltTable.ColumnInfo("i_u_id", VoltType.BIGINT),
        new VoltTable.ColumnInfo("i_num_bids", VoltType.BIGINT),
        new VoltTable.ColumnInfo("i_current_price", VoltType.FLOAT),
        new VoltTable.ColumnInfo("i_end_date", VoltType.TIMESTAMP),
        new VoltTable.ColumnInfo("ib_id", VoltType.BIGINT),
        new VoltTable.ColumnInfo("ib_buyer_id", VoltType.BIGINT),
    };
    
    // -----------------------------------------------------------------
    // STATEMENTS
    // -----------------------------------------------------------------

    public final SQLStmt getItem = new SQLStmt(
        "SELECT i_initial_price, i_current_price, i_num_bids, i_end_date " +
          "FROM " + AuctionMarkConstants.TABLENAME_ITEM + " " + 
         "WHERE i_id = ? AND i_u_id = ? " +
         "  AND i_end_date > ? " +
         "  AND i_status = " + AuctionMarkConstants.ITEM_STATUS_OPEN
    );
    
    public final SQLStmt getMaxBidId = new SQLStmt(
        "SELECT MAX(ib_id) " + 
        "  FROM " + AuctionMarkConstants.TABLENAME_ITEM_BID +
        " WHERE ib_i_id = ? AND ib_u_id = ? "
    );
    
    public final SQLStmt getItemMaxBid = new SQLStmt(
        "SELECT imb_ib_id, ib_bid, ib_max_bid, ib_buyer_id " + 
        "  FROM " + AuctionMarkConstants.TABLENAME_ITEM_MAX_BID + ", " +
                    AuctionMarkConstants.TABLENAME_ITEM_BID +
        " WHERE imb_i_id = ? AND imb_u_id = ? " +
        "   AND imb_ib_id = ib_id AND imb_ib_i_id = ib_i_id AND imb_ib_u_id = ib_u_id "
    );
    
    public final SQLStmt updateItem = new SQLStmt(
        "UPDATE " + AuctionMarkConstants.TABLENAME_ITEM +
        "   SET i_num_bids = i_num_bids + 1, " +
        "       i_current_price = ?, " +
        "       i_updated = ? " +
        " WHERE i_id = ? AND i_u_id = ? "
    );
    
    public final SQLStmt updateItemMaxBid = new SQLStmt(
        "UPDATE " + AuctionMarkConstants.TABLENAME_ITEM_MAX_BID + 
        "   SET imb_ib_id = ?, " +
        "       imb_ib_i_id = ?, " +
        "       imb_ib_u_id = ?, " +
        "       imb_updated = ? " +
        " WHERE imb_i_id = ? " +
        "   AND imb_u_id = ?"
    );
    
    public final SQLStmt updateBid = new SQLStmt(
        "UPDATE " + AuctionMarkConstants.TABLENAME_ITEM_BID + 
        "   SET ib_bid = ?, " +
        "       ib_max_bid = ?, " +
        "       ib_updated = ? " +
        " WHERE ib_id = ? " +
        "   AND ib_i_id = ? " +
        "   AND ib_u_id = ? "
    );
    
    public final SQLStmt insertItemBid = new SQLStmt(
        "INSERT INTO " + AuctionMarkConstants.TABLENAME_ITEM_BID + "(" +
        "ib_id, " +
        "ib_i_id, " +
        "ib_u_id, " + 
        "ib_buyer_id, " +
        "ib_bid, " +
        "ib_max_bid, " +
        "ib_created, " +
        "ib_updated " +
        ") VALUES (" +
        "?, " + // ib_id
        "?, " + // ib_i_id
        "?, " + // ib_u_id
        "?, " + // ib_buyer_id
        "?, " + // ib_bid
        "?, " + // ib_max_bid
        "?, " + // ib_created
        "? "  + // ib_updated
        ")"
    );

    public final SQLStmt insertItemMaxBid = new SQLStmt(
    	"INSERT INTO " + AuctionMarkConstants.TABLENAME_ITEM_MAX_BID + "(" +
    	"imb_i_id, " +
    	"imb_u_id, " +
    	"imb_ib_id, " +
    	"imb_ib_i_id, " +
    	"imb_ib_u_id, " +
    	"imb_created, " +
    	"imb_updated " +
    	") VALUES (" +
        "?, " + // imb_i_id
        "?, " + // imb_u_id
        "?, " + // imb_ib_id
        "?, " + // imb_ib_i_id
        "?, " + // imb_ib_u_id
        "?, " + // imb_created
        "? "  + // imb_updated
        ")"
    );

    public VoltTable run(TimestampType benchmarkStart, long item_id, long seller_id, long buyer_id, double newBidMax) {
        final TimestampType currentTime = AuctionMarkConstants.getScaledTimestamp(benchmarkStart, new TimestampType());
        final boolean debug = LOG.isDebugEnabled();

        // Check to make sure that we can even add a new bid to this item
        // If we fail to get back an item, then we know that the auction is closed
        voltQueueSQL(getItem, item_id, seller_id, currentTime);
        VoltTable[] results = voltExecuteSQL();
        assert (results.length == 1);
        if (results[0].getRowCount() == 0) {
            throw new VoltAbortException("Unable to bid on item: Auction has ended");
        }
        boolean advRow = results[0].advanceRow();
        assert (advRow);
        double i_initial_price = results[0].getDouble(0);
        double i_current_price = results[0].getDouble(1);
        long i_num_bids = results[0].getLong(2);
        TimestampType i_end_date = results[0].getTimestampAsTimestamp(3);
        long newBidId = 0;
        long newBidMaxBuyerId = buyer_id;
        
        // If we existing bids, then we need to figure out whether we are the new highest
        // bidder or if the existing one just has their max_bid bumped up
        if (i_num_bids > 0) {
            if (debug) LOG.debug("Retrieving ITEM_MAX_BID information for " + ItemId.toString(item_id));
            voltQueueSQL(getMaxBidId, item_id, seller_id);
            voltQueueSQL(getItemMaxBid, item_id, seller_id);
            results = voltExecuteSQL();
            
            // Get the next ITEM_BID id for this item
            boolean advanceRow = results[0].advanceRow();
            assert (advanceRow);
            newBidId = results[0].getLong(0) + 1;
            assert(results[0].wasNull() == false);

            // Get the current max bid record for this item
            advanceRow = results[1].advanceRow();
            assert (advanceRow);
            long currentBidId = results[1].getLong(0);
            double currentBidAmount = results[1].getDouble(1);
            double currentBidMax = results[1].getDouble(2);
            long currentBuyerId = results[1].getLong(3);
            assert((int)currentBidAmount == (int)i_current_price) : String.format("%.2f == %.2f", currentBidAmount, i_current_price);
            
            // Check whether this bidder is already the max bidder
            // This means we just need to increase their current max bid amount without
            // changing the current auction price
            if (buyer_id == currentBuyerId) {
                if (newBidMax < currentBidMax) {
                    String msg = String.format("%s is already the highest bidder but is trying to " +
                                               "set a new max bid %.2f that is less than current max bid %.2f",
                                               UserId.toString(buyer_id), newBidMax, currentBidMax);
                    throw new VoltAbortException(msg);
                }
                voltQueueSQL(updateBid, i_current_price, newBidMax, currentTime, currentBidId, item_id, seller_id);
                if (debug) LOG.debug(String.format("Increasing %s's existing current winner %s max bid from %.2f to %.2f",
                                                   ItemId.toString(item_id), UserId.toString(buyer_id), currentBidMax, newBidMax));
            }
            // Otherwise check whether this new bidder's max bid is greater than the current max
            else {
                // The new maxBid trumps the existing guy, so our the buyer_id for this txn becomes the new
                // winning bidder at this time. The new current price is one step above the previous
                // max bid amount 
                if (newBidMax > currentBidMax) {
                    i_current_price = Math.min(newBidMax, currentBidMax + (i_initial_price * AuctionMarkConstants.ITEM_BID_PERCENT_STEP));
                    assert(i_current_price > currentBidMax);
                    voltQueueSQL(updateItemMaxBid, newBidId, item_id, seller_id, currentTime, item_id, seller_id);
                    if (debug) LOG.debug(String.format("Changing %s's new current winning bidder to %s [newMaxBid=%.2f > currentMaxBid=%.2f]",
                                                        ItemId.toString(item_id), UserId.toString(buyer_id), newBidMax, currentBidMax));

                }
                // The current max bidder is still the current one
                // We just need to bump up their bid amount to be at least the bidder's amount
                // Make sure that we don't go over the the currentMaxBidMax, otherwise this would mean
                // that we caused the user to bid more than they wanted.
                else {
                    newBidMaxBuyerId = currentBuyerId;
                    i_current_price = Math.min(currentBidMax, newBidMax + (i_initial_price * AuctionMarkConstants.ITEM_BID_PERCENT_STEP));
                    assert(i_current_price >= newBidMax) : String.format("%.2f > %.2f", i_current_price, newBidMax);
                    voltQueueSQL(updateBid, i_current_price, i_current_price, currentTime, currentBidId, item_id, seller_id);
                    if (debug) LOG.debug(String.format("Keeping %s's current winning bidder as %s but updating current price from %.2f to %.2f",
                                                       ItemId.toString(item_id), UserId.toString(buyer_id), currentBidAmount, i_current_price));
                }
            
                // Always insert an new ITEM_BID record even if BuyerId doesn't become
                // the new highest bidder. We also want to insert a new record even if
                // the BuyerId already has ITEM_BID record, because we want to maintain
                // the history of all the bid attempts
                voltQueueSQL(insertItemBid, newBidId, item_id, seller_id, buyer_id,
                                            i_current_price, newBidMax, currentTime, currentTime);
                voltQueueSQL(updateItem, i_current_price, currentTime, item_id, seller_id);
            }
        }
        // There is no existing max bid record, therefore we can just insert ourselves
        else {
            voltQueueSQL(insertItemBid, newBidId, item_id, seller_id, buyer_id, i_initial_price, newBidMax, currentTime, currentTime);
            voltQueueSQL(insertItemMaxBid, item_id, seller_id, newBidId, item_id, seller_id, currentTime, currentTime);
            voltQueueSQL(updateItem, i_current_price, currentTime, item_id, seller_id);
            if (debug) LOG.debug(String.format("Creating %s's first bid record and marking %s as current winner at %.2f",
                                               ItemId.toString(item_id), UserId.toString(buyer_id), i_current_price));
        }
        
        // Fire off all of our queue
        // We don't have to worry about checking whether the auction ended before we could update it
        // because we use the timestamp of when the txn started, not when we updated it
        results = voltExecuteSQL(true);
        assert (results.length > 0);
       
        // Return back information about the current state of the item auction
        VoltTable ret = new VoltTable(RESULT_COLS);
        ret.addRow(new Object[] {
            // ITEM_ID
            item_id,
            // SELLER_ID
            seller_id,
            // NUM BIDS
            i_num_bids + 1,
            // CURRENT PRICE
            i_current_price,
            // END DATE
            i_end_date,
            // MAX BID ID
            newBidId,
            // MAX BID BUYER_ID
            newBidMaxBuyerId,
        });
        return ret;
    }
}
