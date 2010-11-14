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
            	"SET i_status = 2 " +   
            "WHERE i_id = ? AND i_u_id = ? "
        );    
    
    public final SQLStmt select_item_bid = new SQLStmt(
    		"SELECT ib_buyer_id " + 
    		"FROM " + AuctionMarkConstants.TABLENAME_ITEM_BID + " " + 
    		"WHERE ib_id = ? AND ib_i_id = ? AND ib_u_id = ? "
    	);
    
    public final SQLStmt select_item_max_bid = new SQLStmt(
            "SELECT imb_ib_id, imb_ib_i_id, imb_ib_u_id " + 
    		"FROM " + AuctionMarkConstants.TABLENAME_ITEM_MAX_BID + " " +
            "WHERE imb_i_id = ? AND imb_u_id = ?"
        );
    
    public final SQLStmt select_max_purchase_id = new SQLStmt(
            "SELECT MAX(ip_id) " + 
    		"FROM " + AuctionMarkConstants.TABLENAME_ITEM_PURCHASE + " " +
            "WHERE ip_ib_id = ? AND ip_ib_i_id = ? AND ip_ib_u_id = ?"
        );    
    
    public VoltTable run(long ib_id, long i_id, long u_id,long buyer_id) throws VoltAbortException {
    	
    	// Check if this is the correct max_bid ( max_bid_buyer_id == buyer_id)
    	voltQueueSQL(select_item_max_bid, i_id, u_id);
    	VoltTable results[] = voltExecuteSQL();
    	assert(results.length == 1);
    	if (results[0].getRowCount() == 1) {
    		assert(results[0].advanceRow());
    		long imb_ib_id = results[0].getLong(0);
    		long imb_ib_i_id = results[0].getLong(1);
    		long imb_ib_u_id = results[0].getLong(2);
    		
    		voltQueueSQL(select_item_bid, imb_ib_id, imb_ib_i_id, imb_ib_u_id);
        	results = voltExecuteSQL();
    		
        	boolean advanceRow = results[0].advanceRow(); 
        	assert(advanceRow);
        	
        	long ib_buyer_id = results[0].getLong(0);
    		
    		if(buyer_id != ib_buyer_id){
    	    	throw new VoltAbortException("User " + buyer_id + " did not win the bit " + ib_id);
    		}
    	} else {
    		throw new VoltAbortException("No table return");
    	}
    	
    	long ip_id;
    	
    	// Set item_purchase_id
    	voltQueueSQL(select_max_purchase_id, ib_id, i_id, u_id);
    	results = voltExecuteSQL();
    	assert(results.length == 1);
    	if (results[0].getRowCount() == 0){
    		ip_id = 0;
    	} else {
    		boolean advanceRow = results[0].advanceRow();
    		assert(advanceRow);
    		
    		ip_id = results[0].getLong(0);
    		if(results[0].wasNull()){
    			ip_id = 0;
    		} else {
    			ip_id++;
    		}
    	}
    	
    	// Insert a new purchase
        voltQueueSQL(insert_purchase, ip_id, ib_id, i_id, u_id, new TimestampType());
        voltExecuteSQL();
        
        // Update item status to close
        voltQueueSQL(update_item_status, i_id, u_id);
        voltExecuteSQL();
        
        // Return ip_id, ip_ib_id, ip_ib_i_id, u_id, ip_ib_u_id
        VoltTable ret = new VoltTable(new VoltTable.ColumnInfo[]{
        		new VoltTable.ColumnInfo("ip_id", VoltType.BIGINT),
        		new VoltTable.ColumnInfo("ip_ib_id", VoltType.BIGINT), 
        		new VoltTable.ColumnInfo("ip_ib_i_id", VoltType.BIGINT),
        		new VoltTable.ColumnInfo("u_id", VoltType.BIGINT),
        		new VoltTable.ColumnInfo("ip_ib_u_id", VoltType.BIGINT)
        });
        ret.addRow(new Object[]{ip_id, ib_id, i_id, u_id, buyer_id});
        
        return ret;
    }	
}