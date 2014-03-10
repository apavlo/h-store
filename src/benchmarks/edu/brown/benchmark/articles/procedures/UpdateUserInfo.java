package edu.brown.benchmark.articles.procedures;

import org.voltdb.ProcInfo;
import org.voltdb.SQLStmt;
import org.voltdb.VoltProcedure;
import org.voltdb.VoltTable;
@ProcInfo(
	    partitionInfo = "USERS.U_ID: 0",
	    singlePartition = true
	)
public class UpdateUserInfo extends VoltProcedure{
    public final SQLStmt UpdateUser = new SQLStmt("UPDATE USERS SET u_lastname = ? WHERE U_ID = ?");
    
    public VoltTable[] run(String lastname, long u_id) {
    	System.out.println("Running procedure update user info "+u_id);
        voltQueueSQL(UpdateUser, lastname, u_id);
        return (voltExecuteSQL(true));
    }   

}
