package edu.brown.benchmark.auctionmark.procedures;

import org.voltdb.ProcInfo;
import org.voltdb.SQLStmt;
import org.voltdb.VoltProcedure;
import org.voltdb.VoltTable;
import org.voltdb.VoltType;
import org.voltdb.types.TimestampType;

import edu.brown.benchmark.auctionmark.AuctionMarkConstants;

@ProcInfo (
    partitionInfo = "USER.U_ID: 1",
    singlePartition = true
)
public class NewFeedback extends VoltProcedure{
	
    public final SQLStmt select_max_feedback = new SQLStmt(
            "SELECT MAX(if_id) " + 
            "FROM " + AuctionMarkConstants.TABLENAME_ITEM_FEEDBACK + " " + 
            "WHERE if_i_id = ? AND if_u_id = ?"
        );
	
    public final SQLStmt insert_feedback = new SQLStmt(
            "INSERT INTO " + AuctionMarkConstants.TABLENAME_ITEM_FEEDBACK + "(" +
            	"if_id," +
            	"if_i_id," +
            	"if_u_id," +
            	"if_buyer_id," +
            	"if_rating," +
            	"if_date," +
            	"if_comment" +
            ") VALUES(?,?,?,?,?,?,?)"
        );
    
    public VoltTable run(long i_id, long seller_id, long buyer_id, long rating, String comment) {
    	long if_id;
    	
//    	System.out.println("NewFeedback::: selecting max feedback");
    	
    	// Set comment_id
    	voltQueueSQL(select_max_feedback, i_id, seller_id);
    	VoltTable[] results = voltExecuteSQL();
    	assert(1 == results.length);
    	if(0 == results[0].getRowCount()){
    		if_id = 0;
    	} else {
    		results[0].advanceRow();
    		if_id = results[0].getLong(0) + 1;
    	}
    	
//    	System.out.println("NewFeedback::: if_id = " + if_id);
    	
        voltQueueSQL(insert_feedback, if_id, i_id, seller_id, buyer_id, rating, new TimestampType(), comment);
        voltExecuteSQL();
        
//        System.out.println("NewFeedback::: feedback inserted ");
        
        // Return new if_id
        VoltTable ret = new VoltTable(new VoltTable.ColumnInfo("if_id", VoltType.BIGINT));
        ret.addRow(if_id);
        return ret;
    }	
	
}