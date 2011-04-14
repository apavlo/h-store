/**
 * 
 */
package edu.brown.benchmark.auctionmark.procedures;

import java.util.Date;

import org.voltdb.ProcInfo;
import org.voltdb.SQLStmt;
import org.voltdb.VoltProcedure;
import org.voltdb.VoltTable;
import org.voltdb.VoltType;
import org.voltdb.types.TimestampType;

import edu.brown.benchmark.auctionmark.AuctionMarkConstants;

/**
 * NewBid
 * @author pavlo
 * @author visawee
 */
@ProcInfo (
    partitionInfo = "USER.U_ID: 1",
    singlePartition = true
)
public class NewBid extends VoltProcedure {

    /**
     * -- Store Bid
-- minBid, bid, maxBid???
-- * Assume that bid type 0 = bid, 1 = buy now
UPDATE ITEM SET i_max_bid=$maxBid, i_nb_of_bids = i_nb_of_bids+1 WHERE i_id=$itemId;

INSERT INTO ITEM_BID() 
VALUES ($bidId, $itemId, $sellerId, $buyerId,  $qty, 0, $bid, $maxBid, '$now');
     * 
     */
    
    public final SQLStmt updateItem = new SQLStmt(
        "UPDATE " + AuctionMarkConstants.TABLENAME_ITEM +
        "   SET i_num_bids = i_num_bids + 1 " +     
        " WHERE i_id = ? AND i_u_id = ?" +
        "   AND i_status = " + AuctionMarkConstants.STATUS_ITEM_OPEN
    );

    public final SQLStmt getMaxBidId = new SQLStmt(
            "SELECT MAX(ib_id) " + 
            "  FROM " + AuctionMarkConstants.TABLENAME_ITEM_BID +
            " WHERE ib_i_id = ? AND ib_u_id = ? "
    );
    /*
    public final SQLStmt getItemMaxBid = new SQLStmt(
        "SELECT " + AuctionMarkConstants.TABLENAME_ITEM_BID + ".* " + 
        "  FROM " + AuctionMarkConstants.TABLENAME_ITEM_MAX_BID + ", "
                  + AuctionMarkConstants.TABLENAME_ITEM_BID +
        " WHERE imb_i_id = ? " +
          " AND imb_u_id = ? " +
          " AND imb_ib_id = ib_id " +
          " AND imb_ib_i_id = ib_i_id " +
          " AND imb_ib_u_id = ib_u_id "
    );
    */
    
    public final SQLStmt getItemMaxBid = new SQLStmt(
    		"SELECT imb_ib_id " + 
            "  FROM " + AuctionMarkConstants.TABLENAME_ITEM_MAX_BID + " " + 
            " WHERE imb_i_id = ? " +
              " AND imb_u_id = ? "
    );
    
    public final SQLStmt getItemBid = new SQLStmt(
    		"SELECT ib_bid, ib_max_bid " + 
    		"  FROM " + AuctionMarkConstants.TABLENAME_ITEM_BID + " " + 
    		" WHERE ib_id = ? AND ib_i_id = ? AND ib_u_id = ? "
    ); 
    
    
    public final SQLStmt getItemMaxBid2 = new SQLStmt(
            "SELECT " + " * " + 
            "  FROM " + AuctionMarkConstants.TABLENAME_ITEM_MAX_BID + " " + 
            " WHERE imb_i_id = ? " +
              " AND imb_u_id = ? "
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
            "   SET ib_bid = ? " +
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
 
    public final SQLStmt getUserWatch = new SQLStmt(
            "SELECT uw_u_id FROM " + AuctionMarkConstants.TABLENAME_USER_WATCH + " " +
            " WHERE uw_u_id = ?"
    );
   
    public final SQLStmt insertUserWatch = new SQLStmt(
            "INSERT INTO " + AuctionMarkConstants.TABLENAME_USER_WATCH + "(" +
            "uw_u_id, " +
            "uw_i_id, " +
            "uw_i_u_id, " +
            "imb_ib_i_id, " +
            "uw_created, " +
            ") VALUES (" +
            "?, " + // uw_u_id
            "?, " + // uw_i_id
            "?, " + // uw_i_u_id
            "?, " + // imb_ib_i_id
            "?, " + // imb_ib_u_id
            "?, " + // uw_created
            ")"
        );    
 
    /**
     * 
     * @param i_id
     * @param u_id
     * @param i_buyer_id
     * @param bid
     * @param type
     */
    public VoltTable run(long i_id, long u_id, long i_buyer_id, double bid, double maxBid) {
        Date currentTime = new Date();
        TimestampType currentTimestamp = new TimestampType();
        
        // First check to make sure that we can even add a new bid to this item
        // If we fail to update, the new know that the auction is closed
        voltQueueSQL(updateItem, i_id, u_id);
        VoltTable results[] = voltExecuteSQL();
        assert(1 == results.length);
        boolean advRow = results[0].advanceRow();
        assert(advRow);
        if (results[0].getLong(0) == 0) {
            throw new VoltAbortException("Unable to bid on item: Auction has ended");
        }
        
        voltQueueSQL(getMaxBidId, i_id, u_id);
        results = voltExecuteSQL();
        assert(1 == results.length);
        
        long ib_id;
        
        if(results[0].advanceRow()){
        	// Has 1 or more bids for this item
        	ib_id = results[0].getLong(0);
        	if(results[0].wasNull()){
        		ib_id = 0;
        	} else {
        		ib_id++;
        	}
        } else {
        	// No bid for this item yet
        	ib_id = 0;
        }
        
        // Get the current max bid record for this item
        voltQueueSQL(getItemMaxBid, i_id, u_id);
        VoltTable[] itemMaxBidTable = voltExecuteSQL();
        assert(itemMaxBidTable.length == 1);
        
        // If we have an existing max bid, then we need to figure out whether we are
        // the new highest bidder or if the existing one just has their max_bid bumped up
        if (itemMaxBidTable[0].getRowCount() == 1) {
        	boolean advanceRow = itemMaxBidTable[0].advanceRow(); 
            assert(advanceRow);
            
            long current_bid_id = itemMaxBidTable[0].getLong(0);
        
            voltQueueSQL(getItemBid, current_bid_id, i_id, u_id);
            VoltTable[] itemBidTable = voltExecuteSQL();
            
            assert(1 == itemBidTable.length);
            advanceRow = itemBidTable[0].advanceRow(); 
            assert(advanceRow);
            
            double current_bid = itemBidTable[0].getDouble(0);
            double current_max_bid = itemBidTable[0].getDouble(1);
            boolean newBidWin = false;
            
            if(maxBid > current_max_bid){
            	newBidWin = true;
            	if(bid < current_max_bid){
                    bid = current_max_bid;
            	}
            } else {
            	if(bid > current_bid){
            		voltQueueSQL(updateBid, bid, current_bid_id, i_id, u_id);
            		voltExecuteSQL();
            	}
            }
            
            voltQueueSQL(insertItemBid, ib_id, i_id, u_id, i_buyer_id, bid, maxBid, new TimestampType(currentTime.getTime()), new TimestampType(currentTime.getTime()));
            voltExecuteSQL();
            
            if(newBidWin){
            	voltQueueSQL(updateItemMaxBid, ib_id, i_id, u_id, currentTimestamp, i_id, u_id);
            	voltExecuteSQL();
            }
            
        } else {
        	
            // There is no existing max bid record, therefore we can just insert ourselves
        	voltQueueSQL(insertItemBid, ib_id, i_id, u_id, i_buyer_id, bid, maxBid, currentTimestamp, currentTimestamp);
            voltExecuteSQL();
        	
        	voltQueueSQL(insertItemMaxBid, i_id, u_id, ib_id, i_id, u_id, currentTimestamp, currentTimestamp);
        	voltExecuteSQL();
        	
            // check whether the item has been watched, if not add it to the watch list
            voltQueueSQL(getUserWatch, u_id, i_id, i_buyer_id, new TimestampType(currentTime.getTime()));
            VoltTable[] watched_items = voltExecuteSQL();
            if (watched_items.length == 0) {
                // insert into the watched_items
                voltQueueSQL(insertUserWatch, u_id, i_id, i_buyer_id, new TimestampType(currentTime.getTime()));
                voltExecuteSQL();
            } else {
                assert (1 == watched_items.length) : "The current item is present more than once in the watched items table";
            }
        }
        
        // Return new ib_id
        VoltTable ret = new VoltTable(new VoltTable.ColumnInfo("ib_id", VoltType.BIGINT));
        ret.addRow(ib_id);
        return ret;
    }
}
