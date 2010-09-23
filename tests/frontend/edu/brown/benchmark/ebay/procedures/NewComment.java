package edu.brown.benchmark.ebay.procedures;

import org.voltdb.ProcInfo;
import org.voltdb.SQLStmt;
import org.voltdb.VoltProcedure;
import org.voltdb.VoltTable;
import org.voltdb.VoltType;
import org.voltdb.types.TimestampType;

import edu.brown.benchmark.ebay.EbayConstants;

/**
 * NewComment
 * Description goes here
 * @author visawee
 */
@ProcInfo (
    partitionInfo = "USER.U_ID: 1",
    singlePartition = true
)
public class NewComment extends VoltProcedure{
	
    public final SQLStmt select_max_comment = new SQLStmt(
            "SELECT MAX(ic_id) " + 
            "FROM " + EbayConstants.TABLENAME_ITEM_COMMENT + " " + 
            "WHERE ic_i_id = ? AND ic_u_id = ?"
        );
	
    public final SQLStmt insert_comment = new SQLStmt(
            "INSERT INTO " + EbayConstants.TABLENAME_ITEM_COMMENT + "(" +
            	"ic_id," +
            	"ic_i_id," +
            	"ic_u_id," +
            	"ic_buyer_id," +
            	"ic_date," +
            	"ic_question" +
            ") VALUES(?,?,?,?,?,?)"
        );
	
    public VoltTable run(long i_id, long seller_id, long buyer_id, String question) {
    	
    	long ic_id;
    	
    	// Set comment_id
    	voltQueueSQL(select_max_comment, i_id, seller_id);
    	VoltTable[] results = voltExecuteSQL();
    	assert(1 == results.length);
    	if(0 == results[0].getRowCount()){
    		ic_id = 0;
    	} else {
    		results[0].advanceRow();
    		ic_id = results[0].getLong(0) + 1;
    	}
    	
        voltQueueSQL(insert_comment, ic_id, i_id, seller_id, buyer_id, new TimestampType(), question);
        voltExecuteSQL();
        
        // Return new ic_id
        VoltTable ret = new VoltTable(new VoltTable.ColumnInfo("ic_id", VoltType.BIGINT));
        ret.addRow(ic_id);
        return ret;
    }	
	
}