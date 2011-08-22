package edu.brown.benchmark.auctionmark.procedures;

import org.apache.log4j.Logger;
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
 * PostAuction
 * @author pavlo
 * @author visawee
 */
@ProcInfo(singlePartition = false)
public class CloseAuctions extends VoltProcedure {
    private static final Logger LOG = Logger.getLogger(CloseAuctions.class);

    // -----------------------------------------------------------------
    // STATIC MEMBERS
    // -----------------------------------------------------------------
    
    private static final ColumnInfo[] RESULT_COLS = {
        new ColumnInfo("i_id", VoltType.BIGINT), 
        new ColumnInfo("i_u_id", VoltType.BIGINT),  
        new ColumnInfo("i_num_bids", VoltType.BIGINT),
        new ColumnInfo("i_current_price", VoltType.FLOAT),
        new ColumnInfo("i_end_date", VoltType.TIMESTAMP),
        new ColumnInfo("i_status", VoltType.BIGINT),
        new ColumnInfo("imb_ib_id", VoltType.BIGINT), 
        new ColumnInfo("ib_buyer_id", VoltType.BIGINT),
    };
    
    // -----------------------------------------------------------------
    // STATEMENTS
    // -----------------------------------------------------------------
    
    public final SQLStmt getDueItems = new SQLStmt(
        "SELECT i_id, i_u_id, i_current_price, i_num_bids, i_end_date, i_status " + 
          "FROM " + AuctionMarkConstants.TABLENAME_ITEM + " " + 
         "WHERE (i_start_date BETWEEN ? AND ?) " +
           "AND i_status = " + AuctionMarkConstants.ITEM_STATUS_OPEN + " " +
         "ORDER BY i_id ASC " +
         "LIMIT 25 "
    );
    
    public final SQLStmt getMaxBid = new SQLStmt(
        "SELECT imb_ib_id, ib_buyer_id " + 
          "FROM " + AuctionMarkConstants.TABLENAME_ITEM_MAX_BID + ", " +
                    AuctionMarkConstants.TABLENAME_ITEM_BID +
        " WHERE imb_i_id = ? AND imb_u_id = ? " +
           "AND ib_id = imb_ib_id AND ib_i_id = imb_i_id AND ib_u_id = imb_u_id "
    );
    
    public final SQLStmt updateItemStatus = new SQLStmt(
        "UPDATE " + AuctionMarkConstants.TABLENAME_ITEM + " " +
           "SET i_status = ?, " +
           "    i_updated = ? " +
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
    public VoltTable run(TimestampType benchmarkTimes[], TimestampType startTime, TimestampType endTime) {
        final TimestampType currentTime = AuctionMarkBenchmarkProfile.getScaledTimestamp(benchmarkTimes[0], benchmarkTimes[1], new TimestampType());
        final boolean debug = LOG.isDebugEnabled();

        if (debug) {
            LOG.debug(String.format("startTime=%s, endTime=%s, currentTime=%s",
                                    startTime, endTime, currentTime));
        }

        final VoltTable ret = new VoltTable(RESULT_COLS);
        int closed_ctr = 0;
        int waiting_ctr = 0;
        int round = 10;
        while (round-- > 0) {
            voltQueueSQL(getDueItems, startTime, endTime);
            final VoltTable[] dueItemsTable = voltExecuteSQL();
            assert (1 == dueItemsTable.length);
            if (dueItemsTable[0].getRowCount() == 0) break;
            if (debug)
                LOG.debug(String.format("Round #%02d - Due Items %d / %d\n%s\n",
                                       round, dueItemsTable[0].getRowCount(), (closed_ctr+waiting_ctr), dueItemsTable[0]));

            boolean with_bids[] = new boolean[dueItemsTable[0].getRowCount()];
            Object output_rows[][] = new Object[with_bids.length][];
            int i = 0; 
            while (dueItemsTable[0].advanceRow()) {
                long itemId = dueItemsTable[0].getLong(0);
                long sellerId = dueItemsTable[0].getLong(1);
                double currentPrice = dueItemsTable[0].getDouble(2);
                long numBids = dueItemsTable[0].getLong(3);
                TimestampType endDate = dueItemsTable[0].getTimestampAsTimestamp(4);
                long itemStatus = dueItemsTable[0].getLong(5);
                
                if (debug) LOG.debug(String.format("Getting max bid for itemId=%d / sellerId=%d", itemId, sellerId));
                assert(itemStatus == AuctionMarkConstants.ITEM_STATUS_OPEN);
                
                output_rows[i] = new Object[] { itemId,         // i_id
                                                sellerId,       // i_u_id
                                                numBids,        // i_num_bids
                                                currentPrice,   // i_current_price
                                                endDate,        // i_end_date
                                                itemStatus,     // i_status
                                                null,           // imb_ib_id
                                                null            // ib_buyer_id
                };
                
                // Has bid on this item - set status to WAITING_FOR_PURCHASE
                // We'll also insert a new USER_ITEM record as needed
                if (numBids > 0) {
                    voltQueueSQL(getMaxBid, itemId, sellerId);
                    with_bids[i++] = true;
                    waiting_ctr++;
                }
                // No bid on this item - set status to CLOSED
                else {
                    closed_ctr++;
                    with_bids[i++] = false;
                }
            } // FOR
            final VoltTable[] bidResults = voltExecuteSQL();
            
            // We have to do this extra step because H-Store doesn't have good support in the
            // query optimizer for LEFT OUTER JOINs
            int batch_size = 0;
            int bidResultsCtr = 0;
            for (i = 0; i < with_bids.length; i++) {
                long itemId = (Long)output_rows[i][0];
                long sellerId = (Long)output_rows[i][1];
                long status = (with_bids[i] ? AuctionMarkConstants.ITEM_STATUS_WAITING_FOR_PURCHASE :
                                              AuctionMarkConstants.ITEM_STATUS_CLOSED);
                voltQueueSQL(updateItemStatus, status, currentTime, itemId, sellerId);
                if (debug) LOG.debug(String.format("Updated Status for Item %d => %s",
                                     itemId, (status == AuctionMarkConstants.ITEM_STATUS_CLOSED ? "CLOSED" : "WAITING_FOR_PURCHASE")));
                
                if (with_bids[i]) {
                    final VoltTable vt = bidResults[bidResultsCtr++]; 
                    boolean adv = vt.advanceRow();
                    assert(adv);
                    long bidId = vt.getLong(0);
                    long buyerId = vt.getLong(1);
                    
                    output_rows[i][RESULT_COLS.length-2] = bidId;   // imb_ib_id
                    output_rows[i][RESULT_COLS.length-1] = buyerId; // ib_buyer_id
                    
                    assert (buyerId != -1);
                    voltQueueSQL(insertUserItem, buyerId, itemId, sellerId, currentTime);
                    batch_size++;
                }
                if (++batch_size > AuctionMarkConstants.BATCHSIZE_CLOSE_AUCTIONS_UPDATES) {
                    VoltTable updateResults[] = voltExecuteSQL();
                    for (int j = 0; j < updateResults.length; j++) {
                        VoltTable vt = updateResults[j];
                        assert(vt.getRowCount() == 0);
                        if (vt.asScalarLong() == 0) {
                            String msg = "Failed to process closed auctions\n" + voltLastQueriesExecuted()[j];
                            throw new VoltAbortException(msg);
                        }
                    } // FOR
                    batch_size = 0;
                }
                ret.addRow(output_rows[i]);
            } // FOR
            if (batch_size > 0) voltExecuteSQL();
        } // WHILE

//        if (debug)
            LOG.info(String.format("Updated Auctions - Closed=%d / Waiting=%d", closed_ctr, waiting_ctr));
        return (ret);
    }
}