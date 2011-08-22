package edu.brown.benchmark.auctionmark.procedures;

import org.voltdb.ProcInfo;
import org.voltdb.SQLStmt;
import org.voltdb.VoltProcedure;
import org.voltdb.VoltTable;
import org.voltdb.VoltType;
import org.voltdb.types.TimestampType;

import edu.brown.benchmark.auctionmark.AuctionMarkBenchmarkProfile;
import edu.brown.benchmark.auctionmark.AuctionMarkConstants;
import edu.brown.benchmark.auctionmark.util.ItemId;

/**
 * UpdateItem
 * @author pavlo
 * @author visawee
 */
@ProcInfo (
    partitionInfo = "USER.U_ID: 2",
    singlePartition = true
)
public class UpdateItem extends VoltProcedure{
	
    // -----------------------------------------------------------------
    // STATEMENTS
    // -----------------------------------------------------------------
    
    public final SQLStmt updateItem = new SQLStmt(
        "UPDATE " + AuctionMarkConstants.TABLENAME_ITEM +
        "   SET i_description = ?, " +
        "       i_updated = ? " +
        " WHERE i_id = ? AND i_u_id = ? " +
        "   AND i_status = " + AuctionMarkConstants.ITEM_STATUS_OPEN
    );
    
    public final SQLStmt deleteItemAttribute = new SQLStmt(
        "DELETE FROM " + AuctionMarkConstants.TABLENAME_ITEM_ATTRIBUTE +
        " WHERE ia_id = ? AND ia_i_id = ? AND ia_u_id = ?"
    );

    public final SQLStmt getMaxItemAttributeId = new SQLStmt(
        "SELECT MAX(ia_id) FROM " + AuctionMarkConstants.TABLENAME_ITEM_ATTRIBUTE +
        " WHERE ia_i_id = ? AND ia_u_id = ?"
    );
    
    public final SQLStmt insertItemAttribute = new SQLStmt(
        "INSERT INTO " + AuctionMarkConstants.TABLENAME_ITEM_ATTRIBUTE + "(" +
            "ia_id," + 
            "ia_i_id," + 
            "ia_u_id," + 
            "ia_gav_id," + 
            "ia_gag_id" + 
        ") VALUES(?, ?, ?, ?, ?)"
    );

    // -----------------------------------------------------------------
    // RUN METHOD
    // -----------------------------------------------------------------

	/**
	 * The buyer modifies an existing auction that is still available.
	 * The transaction will just update the description of the auction.
	 * A small percentage of the transactions will be for auctions that are
	 * uneditable (1.0%?); when this occurs, the transaction will abort.
	 */
    public VoltTable run(TimestampType benchmarkTimes[],
                         long item_id, long seller_id, String description,
                         long delete_attribute, long add_attribute[]) {
        final TimestampType currentTime = AuctionMarkBenchmarkProfile.getScaledTimestamp(benchmarkTimes[0], benchmarkTimes[1], new TimestampType());
        voltQueueSQL(updateItem, description, currentTime, item_id, seller_id);
        final VoltTable results[] = voltExecuteSQL(false);
        assert(results.length == 1);
        if (results[0].getRowCount() == 0) {
            throw new VoltAbortException("Unable to update closed auction");
        }
        
        // DELETE ITEM_ATTRIBUTE
        if (delete_attribute != VoltType.NULL_BIGINT) {
            // Only delete the first (if it even exists)
            long ia_id = ItemId.getUniqueElementId(item_id, 0);
            voltQueueSQL(deleteItemAttribute, ia_id, item_id, seller_id);
            voltExecuteSQL(true);
        }
        // ADD ITEM_ATTRIBUTE
        if (add_attribute.length > 0 && add_attribute[0] != VoltType.NULL_BIGINT) {
            assert(add_attribute.length == 2);
            long gag_id = add_attribute[0];
            long gav_id = add_attribute[1];
            long ia_id = -1;
            
            voltQueueSQL(getMaxItemAttributeId, item_id, seller_id);
            final VoltTable attrResults[] = voltExecuteSQL();
            assert(attrResults.length == 1);
            if (attrResults[0].getRowCount() > 0) {
                boolean adv = attrResults[0].advanceRow();
                assert(adv);
                ia_id = attrResults[0].getLong(0);
                assert(attrResults[0].wasNull() == false);
            } else {
                ia_id = ItemId.getUniqueElementId(item_id, 0);
            }
            assert(ia_id > 0);

            voltQueueSQL(insertItemAttribute, ia_id, item_id, seller_id, gag_id, gav_id);
            voltExecuteSQL(true);
        }
        
        return (results[0]);
    }	
	
}