package edu.brown.benchmark.auctionmark.procedures;

import org.apache.log4j.Logger;
import org.voltdb.ProcInfo;
import org.voltdb.SQLStmt;
import org.voltdb.VoltProcedure;
import org.voltdb.VoltTable;
import org.voltdb.VoltType;
import org.voltdb.VoltTable.ColumnInfo;
import org.voltdb.types.TimestampType;

import edu.brown.benchmark.auctionmark.AuctionMarkConstants;
import edu.brown.benchmark.auctionmark.util.ItemId;

/**
 * PostAuction
 * @author pavlo
 * @author visawee
 */
@ProcInfo(singlePartition = false)
public class PostAuction extends VoltProcedure {
    private static final Logger LOG = Logger.getLogger(PostAuction.class);

    // -----------------------------------------------------------------
    // STATIC MEMBERS
    // -----------------------------------------------------------------
    
    private static final ColumnInfo RESULT_COLS[] = {
        new ColumnInfo("closed", VoltType.BIGINT), 
        new ColumnInfo("waiting", VoltType.BIGINT),  
    };
    
    // -----------------------------------------------------------------
    // STATEMENTS
    // -----------------------------------------------------------------
    
    public final SQLStmt updateItemStatus = new SQLStmt(
        "UPDATE " + AuctionMarkConstants.TABLENAME_ITEM + " " +
           "SET i_status = ? " +   
        "WHERE i_id = ? AND i_u_id = ? "
    );

    public final SQLStmt insertUserItem = new SQLStmt(
        "INSERT INTO " + AuctionMarkConstants.TABLENAME_USER_ITEM + "(" +
            "ui_u_id, " +
            "ui_i_id, " +
            "ui_i_u_id, " +  
            "ui_created" +     
        ") VALUES(?, ?, ?, ?)"
    );

    // -----------------------------------------------------------------
    // RUN METHOD
    // -----------------------------------------------------------------
    
    /**
     * @param item_ids - Item Ids
     * @param seller_ids - Seller Ids
     * @param bid_ids - ItemBid Ids
     * @return
     */
    public VoltTable run(long item_ids[], long seller_ids[], long buyer_ids[], long bid_ids[]) {
        final boolean debug = LOG.isDebugEnabled();
        if (debug) {
            LOG.debug("PostAuction::: total rows = " + item_ids.length);
        }

        // Go through each item and update the item status
        // We'll also insert a new USER_ITEM record as needed
        TimestampType timestamp = new TimestampType();
        int closed_ctr = 0;
        int waiting_ctr = 0;
        int batch_size = 0;
        for (int i = 0; i < item_ids.length; i++) {
            long item_id = item_ids[i];
            long seller_id = seller_ids[i];
            long buyer_id = buyer_ids[i];
            long bid_id = bid_ids[i];

            // No bid on this item - set status to CLOSED
            if (bid_id == AuctionMarkConstants.NO_WINNING_BID) {
                if (debug) LOG.debug(String.format("PostAuction::: (%d) %s => CLOSED", i, new ItemId(item_id)));
                this.voltQueueSQL(this.updateItemStatus, AuctionMarkConstants.STATUS_ITEM_CLOSED, item_id, seller_id);
                closed_ctr++;
            }
            // Has bid on this item - set status to WAITING_FOR_PURCHASE
            else {
                if (debug) LOG.debug(String.format("PostAuction::: (%d) %s => WAITING_FOR_PURCHASE", i, new ItemId(item_id)));
                this.voltQueueSQL(this.updateItemStatus, AuctionMarkConstants.STATUS_ITEM_WAITING_FOR_PURCHASE, item_id, seller_id);
                assert (buyer_id != -1);
                this.voltQueueSQL(this.insertUserItem, buyer_id, item_id, seller_id, timestamp);
                waiting_ctr++;
            }

            if (++batch_size > AuctionMarkConstants.BATCHSIZE_POST_AUCTION) {
                this.voltExecuteSQL();
                batch_size = 0;
            }
        } // FOR
        if (batch_size > 0) this.voltExecuteSQL();

        if (debug)
            LOG.debug(String.format("PostAuction::: closed=%d / waiting=%d", closed_ctr, waiting_ctr));
        VoltTable results = new VoltTable(RESULT_COLS);
        results.addRow(closed_ctr, waiting_ctr);
        return (results);
    }
}