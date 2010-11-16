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

/**
 * PostAuction Description goes here...
 * 
 * @author visawee
 */
@ProcInfo(singlePartition = false)
public class PostAuction extends VoltProcedure {
    protected static final Logger LOG = Logger.getLogger(PostAuction.class);

    public final SQLStmt update_item_status = new SQLStmt(
        "UPDATE " + AuctionMarkConstants.TABLENAME_ITEM + " " +
           "SET i_status = ? " +   
        "WHERE i_id = ? AND i_u_id = ? "
    );

    public final SQLStmt insert_useritem = new SQLStmt(
        "INSERT INTO " + AuctionMarkConstants.TABLENAME_USER_ITEM + "(" +
            "ui_u_id, " +
            "ui_i_id, " +
            "ui_i_u_id, " +  
            "ui_created" +     
        ") VALUES(?, ?, ?, ?)"
    );
    
    private static final ColumnInfo[] returnColumnInfo = {
        new ColumnInfo("closed", VoltType.BIGINT), 
        new ColumnInfo("waiting", VoltType.BIGINT),  
    };

    /**
     * @param i_ids - Item Ids
     * @param seller_ids - Seller Ids
     * @param ib_ids - ItemBid Ids
     * @return
     */
    public VoltTable[] run(long i_ids[], long seller_ids[], long buyer_ids[], long ib_ids[]) {
        final boolean debug = LOG.isDebugEnabled();
        if (debug) {
            LOG.debug("PostAuction::: total rows = " + i_ids.length);
        }

        final TimestampType timestamp = new TimestampType();

        // Go through each item and update the item status
        // We'll also insert a new USER_ITEM record as needed
        int closed_ctr = 0;
        int waiting_ctr = 0;
        int batch_size = 0;
        for (int i = 0; i < i_ids.length; i++) {
            long ib_id = ib_ids[i];
            long i_id = i_ids[i];
            long seller_id = seller_ids[i];
            long buyer_id = buyer_ids[i];

            // No bid on this item - set status to close (2)
            // System.out.println("PostAuction::: (" + i +
            // ") updating item status to 2 (" + i_id + "," + i_u_id + ")");
            if (-1 == ib_id) {
                this.voltQueueSQL(this.update_item_status, AuctionMarkConstants.STATUS_ITEM_CLOSED, i_id, seller_id);
                closed_ctr++;

            // Has bid on this item - set status to wait for purchase (1)
            // System.out.println("PostAuction::: (" + i +
            // ") updating item status to 1 (" + i_id + "," + i_u_id + ")");
            } else {
                this.voltQueueSQL(this.update_item_status, AuctionMarkConstants.STATUS_ITEM_WAITING_FOR_PURCHASE, i_id, seller_id);
                assert(buyer_id != -1);
                this.voltQueueSQL(this.insert_useritem, buyer_id, i_id, seller_id, timestamp);
                if (debug) LOG.debug(String.format("Inserting USER_ITEM: (%d, %d, %d, %s)", buyer_id, i_id, seller_id, timestamp.toString()));
                waiting_ctr++;
            }

            if (++batch_size > 10) {
                this.voltExecuteSQL();
                batch_size = 0;
            }
        } // FOR
        if (batch_size > 0) {
            this.voltExecuteSQL();
        }

        final VoltTable[] results = new VoltTable[] { new VoltTable(returnColumnInfo) };
        results[0].addRow(closed_ctr, waiting_ctr);
        // System.out.println("PostAuction::: finish update\n" + results[0]);
        return (results);
    }
}