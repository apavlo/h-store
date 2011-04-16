package edu.brown.benchmark.auctionmark.procedures;

import java.util.Arrays;

import org.voltdb.ProcInfo;
import org.voltdb.SQLStmt;
import org.voltdb.VoltProcedure;
import org.voltdb.VoltTable;
import org.voltdb.VoltType;
import org.voltdb.types.TimestampType;

import edu.brown.benchmark.auctionmark.AuctionMarkConstants;

/**
 * NewItem
 * Description goes here...
 * @author visawee
 */
@ProcInfo (
    partitionInfo = "USER.U_ID: 1",
    singlePartition = true
)
public class NewItem extends VoltProcedure{
	
	private static final long ITEM_ID_MASK = 0x0FFFFFFFFFFFFFFFl; 
	
    public final SQLStmt insert_item = new SQLStmt(
            "INSERT INTO " + AuctionMarkConstants.TABLENAME_ITEM + "(" +
            	"i_id," + 
            	"i_u_id," + 
            	"i_c_id," + 
            	"i_name," + 
            	"i_description," + 
            	"i_user_attributes," + 
            	"i_initial_price," + 
            	"i_num_bids," + 
            	"i_num_images," + 
            	"i_num_global_attrs," + 
            	"i_start_date," + 
            	"i_end_date" 
            +") VALUES(?,?,?,?,?,?,?,?,?,?,?,?)"
        );
    
    public final SQLStmt select_category = new SQLStmt(
            "SELECT * FROM " + AuctionMarkConstants.TABLENAME_CATEGORY + " WHERE c_id = ? "
        );
    public final SQLStmt select_category_parent = new SQLStmt(
           "SELECT * FROM " + AuctionMarkConstants.TABLENAME_CATEGORY + " WHERE c_parent_id = ? "
      );
    
    public final SQLStmt select_global_attribute = new SQLStmt(
          "SELECT gag_name, gav_name, gag_c_id " +
            "FROM " + AuctionMarkConstants.TABLENAME_GLOBAL_ATTRIBUTE_GROUP + ", " +
                      AuctionMarkConstants.TABLENAME_GLOBAL_ATTRIBUTE_VALUE +
          " WHERE gav_id = ? AND gav_gag_id = ? " +
             "AND gav_gag_id = gag_id"
        );
    
    public final SQLStmt insert_item_attribute = new SQLStmt(
    		"INSERT INTO " + AuctionMarkConstants.TABLENAME_ITEM_ATTRIBUTE + "(" +
    			"ia_id," + 
    			"ia_i_id," + 
    			"ia_u_id," + 
    			"ia_gav_id," + 
    			"ia_gag_id" + 
    		") VALUES(?, ?, ?, ?, ?)"
    	);

    public final SQLStmt insert_image = new SQLStmt(
    		"INSERT INTO " + AuctionMarkConstants.TABLENAME_ITEM_IMAGE + "(" +
    			"ii_id," + 
    			"ii_i_id," + 
    			"ii_u_id," + 
    			"ii_path" + 
    		") VALUES(?, ?, ?, ?)"
    	);
    
    public final SQLStmt decrement_user_balance = new SQLStmt(
    		"UPDATE " + AuctionMarkConstants.TABLENAME_USER + " " +
    		"SET u_balance = u_balance - 1 " + 
    		"WHERE u_id = ?"
    	);
    
    /*
     * Question : Are we considering about existing gag, gav or not?
     * 			: After these records are inserted, the transaction then updates 
     * 			  the USER record to add the listing fee to the seller's balance?
     * 				USER.add_balance
     * 			: Should we add start date and end date?
     */
    
	/*
	 * Insert a new ITEM record for a user. The benchmark client provides all of the preliminary information 
	 * required for the new item, as well as optional information to create derivative image and attribute records.
	 * After inserting the new ITEM record, the transaction then inserts any GLOBAL ATTRIBUTE VALUE and
	 * ITEM IMAGE. The unique identifer for each of these records is a composite 64-bit key where the lower
	 * 60-bits are the i id parameter and the upper 4-bits are used to represent the index of the image/attribute.
	 * For example, if the i id is 100 and there are four items, then the composite key will be 0 100 for the first
	 * image, 1 100 for the second, and so on. After these records are inserted, the transaction then updates
	 * the USER record to add the listing fee to the seller's balance.
	 */
    public VoltTable run(long i_id,
                         long u_id,
                         long c_id,
                         String name,
                         String description,
                         double initial_price,
                         String attributes,
                         long gag_ids[],
                         long gav_ids[],
                         String images[],
                         TimestampType start_date,
                         TimestampType end_date) {

        /*
        System.out.println("NewItem :: run ");
        System.out.println(">> i_id = " + i_id + " , u_id = " + u_id + ", c_id = " + c_id);
        System.out.println("name = " + name + " , description length = " + description.length());
        System.out.println("initial_price = " + initial_price + " , attributes length = " + attributes.length());
        System.out.println("gag_ids[].length = " + gag_ids.length + " , gav_ids[] length = " + gav_ids.length);
        System.out.println("image length = " + images.length + " ");
        System.out.println("start = " + start_date + ", end = " + end_date);
        */
        
        // Get attribute names and append them to the item description
        String names[] = new String[gag_ids.length];
        for (int i = 0; i < gag_ids.length; i++) {
            voltQueueSQL(select_global_attribute, gav_ids[i], gag_ids[i]);
        }
        VoltTable results[] = voltExecuteSQL();
        assert(results.length == gag_ids.length);
        for (int i = 0; i < gag_ids.length; i++) {
            boolean adv = results[i].advanceRow();
            assert(adv);
            names[i] = results[i].getString(0) + results[i].getString(0);
        }
        description += "\n" + Arrays.toString(names);
        
        // Then get the category (plus the parent)
        voltQueueSQL(select_category, c_id);
        voltQueueSQL(select_category_parent, c_id);
        VoltTable category_results[] = voltExecuteSQL();
        assert(category_results.length == 2);
        boolean advanceRow = category_results[0].advanceRow(); 
        assert(advanceRow);
        String category_name = "";
        
        // Parent Name
        if (category_results[1].getRowCount() > 0) {
            advanceRow = category_results[1].advanceRow(); 
            assert(advanceRow);
            category_name += category_results[1].getString(1);
        } else {
            category_name += "<ROOT>";
        }
        
        // Base Category Name
        category_name += " >> " + category_results[0].getString(1);
        description += "\nCATEGORY: " + category_name;
        
        // Insert New Item
        voltQueueSQL(insert_item, i_id, u_id, c_id, name, description, attributes, initial_price, 0, images.length, gav_ids.length, start_date, end_date);
        
//        System.out.println("NewItem :: queue sql ");
        
        voltExecuteSQL();
        
//        System.out.println("NewItem :: executed sql ");
        
        for(int i=0; i<gav_ids.length; i++){
        	long ia_id = ((long)i << 60) | (i_id & ITEM_ID_MASK);
        	voltQueueSQL(insert_item_attribute, ia_id, i_id, u_id, gag_ids[i], gag_ids[i]);
    		voltExecuteSQL();
    	}

    	for(int i=0; i<images.length; i++){
    		long ii_id = ((long)i << 60) | (i_id & ITEM_ID_MASK);
    		voltQueueSQL(insert_image, ii_id, i_id, u_id, images[i]);
    		voltExecuteSQL();
    	}
    	
    	//Update listing fee
    	voltQueueSQL(decrement_user_balance, u_id);
		voltExecuteSQL();
		
        // Return new item_id and user_id
        VoltTable ret = new VoltTable(new VoltTable.ColumnInfo[]{
        		new VoltTable.ColumnInfo("i_id", VoltType.BIGINT), 
        		new VoltTable.ColumnInfo("i_u_id", VoltType.BIGINT)
        });
        ret.addRow(new Object[]{i_id, u_id});
        return ret;
    }
}