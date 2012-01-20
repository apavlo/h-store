package edu.brown.benchmark.auctionmark.procedures;

import org.voltdb.SQLStmt;
import org.voltdb.VoltProcedure;
import org.voltdb.VoltTable;

import edu.brown.benchmark.auctionmark.AuctionMarkConstants;
import edu.brown.benchmark.auctionmark.AuctionMarkConstants.ItemStatus;

public class LoadConfig extends VoltProcedure {

    public final SQLStmt getConfigProfile = new SQLStmt(
        "SELECT * FROM " + AuctionMarkConstants.TABLENAME_CONFIG_PROFILE
    );
    
    public final SQLStmt getItemCategoryCounts = new SQLStmt(
        "SELECT COUNT(i_id) " +
        "  FROM " + AuctionMarkConstants.TABLENAME_ITEM +
        " GROUP BY i_c_id"
    );
    
    public final SQLStmt getItems = new SQLStmt(
        "SELECT i_id, i_current_price, i_end_date, i_num_bids, i_status " +
        "  FROM " + AuctionMarkConstants.TABLENAME_ITEM + 
        " WHERE i_status = ? " +
        " ORDER BY i_iattr0 " +
        " LIMIT " + AuctionMarkConstants.ITEM_ID_CACHE_SIZE
    );
    
    public final SQLStmt getGlobalAttributeGroups = new SQLStmt(
        "SELECT gag_id FROM " + AuctionMarkConstants.TABLENAME_GLOBAL_ATTRIBUTE_GROUP 
    );
    
    public VoltTable[] run() {
        voltQueueSQL(getConfigProfile);
        voltQueueSQL(getItemCategoryCounts);
        
        for (ItemStatus status : ItemStatus.values()) {
            if (status.isInternal()) continue;
            voltQueueSQL(getItems, status.ordinal());
        } // FOR

        voltQueueSQL(getGlobalAttributeGroups);
        
        return voltExecuteSQL(true);
    }
}
