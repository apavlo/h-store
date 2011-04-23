package edu.brown.benchmark.auctionmark.procedures;

import org.voltdb.ProcInfo;
import org.voltdb.SQLStmt;
import org.voltdb.VoltProcedure;
import org.voltdb.VoltTable;
import org.voltdb.types.TimestampType;

import edu.brown.benchmark.auctionmark.AuctionMarkConstants;

/**
 * NewUser
 * Description goes here...
 * @author visawee
 */
@ProcInfo (
    partitionInfo = "USER.U_ID: 0",
    singlePartition = true
)
public class NewUser extends VoltProcedure{
	
	public final SQLStmt select_users = new SQLStmt(
			"SELECT u_id FROM " + AuctionMarkConstants.TABLENAME_USER
	);
	
    public final SQLStmt insert_user = new SQLStmt(
            "INSERT INTO " + AuctionMarkConstants.TABLENAME_USER + "(" + 
            	"u_id," + 
            	"u_rating," + 
            	"u_balance," + 
            	"u_created," + 
            	"u_r_id," + 
            	"u_sattr0, u_sattr1, u_sattr2, u_sattr3, u_sattr4, u_sattr5, u_sattr6, u_sattr7" +
            ") VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
    );
	
    public VoltTable[] run(long u_id, long u_r_id, String[] attributes) {
//    	System.out.println("INSERTING new user ::: u_id = " + u_id);
    	
    	/*
    	voltQueueSQL(select_users);
    	
    	VoltTable[] ret = voltExecuteSQL();
    	
    	while(ret[0].advanceRow()){
    		System.out.println("Existing user id = " + ret[0].getLong(0));
    	}
    	*/
    	
        voltQueueSQL(insert_user, u_id, 0, 0, new TimestampType(), u_r_id, 
        		attributes[0],
        		attributes[1],
        		attributes[2],
        		attributes[3],
        		attributes[4],
        		attributes[5],
        		attributes[6],
        		attributes[7]);
//      System.out.println("user inserted");
        return (voltExecuteSQL());
        

    }	
}