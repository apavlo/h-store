package edu.brown.benchmark.ebay.procedures;

import java.util.List;

import org.voltdb.ProcInfo;
import org.voltdb.SQLStmt;
import org.voltdb.VoltProcedure;
import org.voltdb.VoltTable;
import org.voltdb.VoltType;
import org.voltdb.VoltTable.ColumnInfo;

import edu.brown.benchmark.ebay.EbayConstants;

/**
 * PostAuction
 * Description goes here...
 * @author visawee
 */
@ProcInfo (
    singlePartition = false
)
public class PostAuction extends VoltProcedure{
	
	public final SQLStmt update_item_status = new SQLStmt(
            "UPDATE " + EbayConstants.TABLENAME_ITEM + " " +
            	"SET i_status = ? " +   
            "WHERE i_id = ? AND i_u_id = ? "
    );
	
    private static final ColumnInfo[] returnColumnInfo = {
        new ColumnInfo("closed", VoltType.BIGINT), 
        new ColumnInfo("waiting", VoltType.BIGINT),  
    };
    
    public VoltTable[] run(long i_ids[], long i_u_ids[], long ib_ids[]) {

//    	System.out.println("PostAuction::: total rows = " + i_ids.length);

    	
    	int closed_ctr = 0;
    	int waiting_ctr = 0;
    	int batch_size = 0;
    	for(int i=0; i<i_ids.length; i++){
    		
    		long i_id = i_ids[i];
    		long i_u_id = i_u_ids[i];
    		long ib_id = ib_ids[i];
    		
    		if(-1 == ib_id){
    			// No bid on this item - set status to close (2)
    			// System.out.println("PostAuction::: (" + i + ") updating item status to 2 (" + i_id + "," + i_u_id + ")");
    			voltQueueSQL(update_item_status, 2, i_id, i_u_id);
	    		closed_ctr++;
    		} else {
    			// Has bid on this item - set status to wait for purchase (1)
    			// System.out.println("PostAuction::: (" + i + ") updating item status to 1 (" + i_id + "," + i_u_id + ")");
    			voltQueueSQL(update_item_status, 1, i_id, i_u_id);
    			waiting_ctr++;
    		}
    		
    		if (++batch_size > 10) {
    		    voltExecuteSQL();
    		    batch_size = 0;
    		}
    	} // FOR
    	if (batch_size > 0) voltExecuteSQL();
    	
    	final VoltTable[] results = new VoltTable[] { new VoltTable(returnColumnInfo) };
    	results[0].addRow(closed_ctr, waiting_ctr);
//    	System.out.println("PostAuction::: finish update\n" + results[0]);
        return (results);
    }	
}