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
 * CheckWinningBids
 * <Add description>
 * @author visawee
 */
@ProcInfo (
    partitionInfo = "USER.U_ID: 2",
    singlePartition = false
)
public class CheckWinningBids extends VoltProcedure {
    private static final Logger LOG = Logger.getLogger(CheckWinningBids.class);
	
	/*
	public final SQLStmt select_due_items = new SQLStmt(
		"SELECT i_id, i_u_id " + 
		"FROM " + AuctionMarkConstants.TABLENAME_ITEM + " " + 
		"WHERE i_start_date >= ? AND i_end_date <= ? AND i_u_id >= ? AND i_u_id < ? "
	);
	*/
	
	public final SQLStmt select_due_items = new SQLStmt(
		"SELECT i_id, i_u_id, i_status " + 
		  "FROM " + AuctionMarkConstants.TABLENAME_ITEM + " " + 
		 "WHERE (i_start_date BETWEEN ? AND ?) " +
		   "AND i_u_id >= ? AND i_u_id < ? " +
		   "AND i_status = " + AuctionMarkConstants.STATUS_ITEM_OPEN + " " +
		 "LIMIT 100 "
	);
	
	public final SQLStmt select_max_bid = new SQLStmt(
			"SELECT imb_ib_id " + 
			"FROM " + AuctionMarkConstants.TABLENAME_ITEM_MAX_BID + " " +  
			"WHERE imb_i_id = ? AND imb_u_id = ? "
	);
	
	public final SQLStmt select_item_bid = new SQLStmt(
    		"SELECT ib_buyer_id " + 
    		"FROM " + AuctionMarkConstants.TABLENAME_ITEM_BID + " " + 
    		"WHERE ib_id = ? AND ib_i_id = ? AND ib_u_id = ? "
    	);
	
	/*
	public final SQLStmt select_max_bid = new SQLStmt(
		"SELECT ib_buyer_id, imb_ib_id " + 
		"FROM " + AuctionMarkConstants.TABLENAME_ITEM_MAX_BID + ", " + 
		AuctionMarkConstants.TABLENAME_ITEM_BID + " " + 
		"WHERE imb_ib_id = ib_id AND imb_ib_i_id = ib_i_id AND imb_ib_u_id = ib_u_id " +
		"AND imb_i_id = ? AND imb_u_id = ? "
		
	);
	*/
	/*
    public final SQLStmt select_due_items_with_bids = new SQLStmt(
    	"SELECT i_id, i_u_id, ib_id, ib_buyer_id, imb_ib_id " + 
        "FROM " + AuctionMarkConstants.TABLENAME_ITEM + ", " + 
        " " + AuctionMarkConstants.TABLENAME_ITEM_BID + ", " + 
        " " + AuctionMarkConstants.TABLENAME_ITEM_MAX_BID + " " + 
        "WHERE i_id = ib_i_id AND i_u_id = ib_u_id " + 
        " AND ib_id = imb_ib_id AND ib_u_id = imb_u_id " + 
        " AND i_start_date >= ? AND i_end_date <= ? AND i_u_id >= ? AND i_u_id < ? " 
    );
    */
    
    /*
     * "LEFT OUTER JOIN " + AuctionMarkConstants.TABLENAME_ITEM_BID + " ON i_id = ib_i_id AND i_u_id = ib_u_id " +
        "LEFT OUTER JOIN " + AuctionMarkConstants.TABLENAME_ITEM_MAX_BID + " ON ib_id = imb_ib_id AND ib_u_id = imb_u_id " + 
        "WHERE i_start_date >= ? AND i_end_date <= ? AND i_u_id >= ? AND i_u_id < ?"
     */

    private final ColumnInfo[] columnInfo = {
        new ColumnInfo("i_id", VoltType.BIGINT), 
        new ColumnInfo("i_u_id", VoltType.BIGINT),  
        new ColumnInfo("i_status", VoltType.BIGINT), 
        new ColumnInfo("imb_ib_id", VoltType.BIGINT), 
        new ColumnInfo("ib_buyer_id", VoltType.BIGINT),
    };

    /**
     * 
     * @param startTime
     * @param endTime
     * @param clientId
     * @param clientMaxElements
     * @return
     */
    public VoltTable run(TimestampType startTime, TimestampType endTime, int clientId, long clientMaxElements) {
        final boolean debug = LOG.isDebugEnabled();
        
        long startUserId = ((long)(clientId - 1)) * clientMaxElements;
        long endUserId = ((long)clientId) * clientMaxElements;
        
        if (debug) {
            LOG.debug("CheckWinningBids::: startUserId = " + startUserId);
            LOG.debug("CheckWinningBids::: endUserId = " + endUserId);
            LOG.debug("CheckWinningBids::: startTime = " + startTime);
            LOG.debug("CheckWinningBids::: endTime = " + endTime);
        }
        
        /*
        voltQueueSQL(select_all_items);
        VoltTable[] x = voltExecuteSQL();
        
        System.out.println("S All items");
        while(x[0].advanceRow()){
        	System.out.println(x[0].getLong(0) + "," + x[0].getLong(1) + "," + x[0].getLong(2) + "," + x[0].getTimestampAsTimestamp(3));
        }
        System.out.println("E All items");
        */
        
        voltQueueSQL(select_due_items, startTime, endTime, startUserId, endUserId);
        VoltTable[] dueItemsTable = voltExecuteSQL();
        assert(1 == dueItemsTable.length);
        
        if (debug) LOG.debug("CheckWinningBids::: total due items = " + dueItemsTable[0].getRowCount());
        
        final VoltTable ret = new VoltTable(columnInfo);
        
        while(dueItemsTable[0].advanceRow()){
        	long itemId = dueItemsTable[0].getLong(0);
        	long userId = dueItemsTable[0].getLong(1);
        	long itemStatus = dueItemsTable[0].getLong(2);
        	//System.out.println("CheckWinningBids::: getting max bid for itemId = " + itemId + " : userId = " + userId);
        	
        	voltQueueSQL(select_max_bid, itemId, userId);
            VoltTable[] maxBidTable = voltExecuteSQL();
            assert(1 == maxBidTable.length);            
            
            if(maxBidTable[0].advanceRow()){
            	//System.out.println("CheckWinningBids::: found max bid");
            	long maxBidId = maxBidTable[0].getLong(0);
            	
                voltQueueSQL(select_item_bid, maxBidId, itemId, userId);
                VoltTable[] bidTable = voltExecuteSQL();
                assert(1 == bidTable.length);
                boolean adv = bidTable[0].advanceRow();
                assert(adv);
            	
                long buyerId = bidTable[0].getLong(0);
//                System.out.println("CheckWinningBids::: buyerId = " + buyerId);
                
            	//System.out.println("CheckWinningBids::: adding row itemId = " + itemId + " , userId = " + userId + " , maxBidId = " + maxBidId);
            	ret.addRow(new Object[]{itemId, userId, itemStatus, maxBidId, buyerId});
            } else {
            	//System.out.println("CheckWinningBids::: cannot found max bid");
            	//System.out.println("CheckWinningBids::: adding row itemId = " + itemId + " , userId = " + userId);
            	ret.addRow(new Object[]{itemId, userId, itemStatus, null, null});
            }
        }
        
        if (debug) LOG.debug("CheckWinningBids::: ret row count = " + ret.getRowCount());
        
        /*
        voltQueueSQL(select_due_items_with_bids, startTime, endTime, startUserId, endUserId);
        
        System.out.println("CheckWinningBids::: queue sql");
        
        ret = voltExecuteSQL();
        assert(1 == ret.length);
        
        HashSet<UserItem> itemsWithBids = new HashSet<UserItem>();
        
        System.out.println("CheckWinningBids::: xxx-xxx");
        
        while(ret[0].advanceRow()){
        	long itemId = ret[0].getLong(0);
        	long userId = ret[0].getLong(1);
        	itemsWithBids.add(new UserItem(userId, itemId));
        }
        
        System.out.println("CheckWinningBids::: numRows1 = " + ret[0].getRowCount());
        
        voltQueueSQL(select_due_items, startTime, endTime, startUserId, endUserId);
        VoltTable[] tab = voltExecuteSQL();
        assert(1 == tab.length);
        
        System.out.println("CheckWinningBids::: numRows2 = " + tab[0].getRowCount());
        
        while(tab[0].advanceRow()){
        	long itemId = tab[0].getLong(0);
        	long userId = tab[0].getLong(1);

        	if(!itemsWithBids.contains(new UserItem(userId, itemId))){
        		ret[0].addRow(new Object[]{
        			itemId, userId, null, null, null
        		});
        	}
        }
        ret[0].resetRowPosition();
        
        System.out.println("CheckWinningBids::: numRows3 = " + ret[0].getRowCount());
        */
        
        return ret;
    }	

    class UserItem implements Comparable<UserItem> {

    	private long _userId;
		private long _itemId;
    	
    	public UserItem(long userId, long itemId){
    		this._userId = userId;
    		this._itemId = itemId;
    	}
    	
    	public long getUserId() {
			return _userId;
		}

		public long getItemId() {
			return _itemId;
		}
    	
		public int hashCode(){
			return (new Long(_userId)).hashCode() + (new Long(_itemId)).hashCode(); 
		}
		
		@Override
		public int compareTo(UserItem other) {
			if(_userId == other.getUserId()){
				if(_itemId == other.getItemId()){
					return 0;
				} else {
					return _itemId > other.getItemId()? 1: -1;
				}
			} else {
				return _userId > other.getUserId()? 1: -1;
			}
		}
    }
}