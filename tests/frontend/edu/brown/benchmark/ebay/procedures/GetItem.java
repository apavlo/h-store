package edu.brown.benchmark.ebay.procedures;

import org.voltdb.ProcInfo;
import org.voltdb.SQLStmt;
import org.voltdb.VoltProcedure;
import org.voltdb.VoltTable;

import edu.brown.benchmark.ebay.EbayConstants;

/**
 * Get Item Information
 * Returns all of the attributes for a particular item
 * @author pavlo
 */
@ProcInfo (
    partitionInfo = "USER.U_ID: 1",
    singlePartition = true
)
public class GetItem extends VoltProcedure {

    public final SQLStmt select_item = new SQLStmt(
        "SELECT i_id, i_u_id, i_initial_price, i_current_price FROM " + EbayConstants.TABLENAME_ITEM + " " + 
        "WHERE i_id = ? AND i_u_id = ?  AND i_status = 0"
    );
    
    public final SQLStmt select_user = new SQLStmt(
        "SELECT u_id, u_rating, u_created, u_sattr0, u_sattr1, u_sattr2, u_sattr3, u_sattr4, r_name " +
        "  FROM " + EbayConstants.TABLENAME_USER + ", " +
                    EbayConstants.TABLENAME_REGION +
        " WHERE u_id = ? AND u_r_id = r_id"
    );

     
    //"WHERE i_id = ? AND i_u_id = ?  AND i_status = 0"
    
    public VoltTable[] run(long i_id, long i_u_id) {
    	// TODO: fix loader
    	
        voltQueueSQL(select_item, i_id, i_u_id);
        voltQueueSQL(select_user, i_u_id);
        
    	//voltQueueSQL(select_item);
        return (voltExecuteSQL());
    }
    
}
