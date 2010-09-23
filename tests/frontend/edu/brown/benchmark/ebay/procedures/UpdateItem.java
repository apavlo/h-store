package edu.brown.benchmark.ebay.procedures;

import org.voltdb.ProcInfo;
import org.voltdb.SQLStmt;
import org.voltdb.VoltProcedure;
import org.voltdb.VoltTable;

import edu.brown.benchmark.ebay.EbayConstants;

/**
 * UpdateItem
 * Description goes here...
 * @author visawee
 */
@ProcInfo (
    partitionInfo = "USER.U_ID: 1",
    singlePartition = true
)
public class UpdateItem extends VoltProcedure{
	
    public final SQLStmt update_item = new SQLStmt(
            "UPDATE " + EbayConstants.TABLENAME_ITEM + " " +
            	"SET i_description = ? " +
            "WHERE i_id = ? AND i_u_id = ?"
        );

    // We should do a join on ITEM_ATTRIBUTES and GLOBAL_ATTRIBUTE_GROUP here
//    public final SQLStmt select_item_attributes = new SQLStmt(""
//        );
    
    
	/*
	 * The buyer modifies an existing auction that is still available. The transaction will just update the description 
	 * of the auction. A small percentage of the transactions will be for auctions that are uneditable (1.0%?); when 
	 * this occurs, the transaction will abort. // TODO
	 */
    public VoltTable[] run(long i_id, long i_u_id, String description) {
        voltQueueSQL(update_item, description, i_id, i_u_id);
        return (voltExecuteSQL());
    }	
	
}