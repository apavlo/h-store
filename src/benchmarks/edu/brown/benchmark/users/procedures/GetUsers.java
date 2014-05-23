package edu.brown.benchmark.users.procedures;

import org.voltdb.ProcInfo;
import org.voltdb.SQLStmt;
import org.voltdb.VoltProcedure;
import org.voltdb.VoltTable;

@ProcInfo(
	    singlePartition = false
	)
public class GetUsers extends VoltProcedure{
//	-- c_id            Comment's ID
//	-- a_id            Article's ID
//	-- u_id            User's ID
//	-- c_text            Actual comment text
    public final SQLStmt GetUser = new SQLStmt("SELECT u_attr01 FROM USERS WHERE U_ID=?");
    public final SQLStmt UpdateUsers = new SQLStmt("UPDATE USERS SET u_attr01=? WHERE U_ID=?");

    public VoltTable[] run(long u_id, long u_id2) {
//    	System.out.println("Running procedure Update "+u_id);
    	
        voltQueueSQL(GetUser, u_id);
        VoltTable[] results = voltExecuteSQL();
        boolean adv = results[0].advanceRow();
        //assert (adv);
//        System.out.println(results[0]);
        Long attr1 = results[0].getLong(0);
        voltQueueSQL(UpdateUsers, attr1, u_id2);
        results = voltExecuteSQL(true); 
        assert results.length == 1;
        return results;
    }   

	
    
//    public final SQLStmt GetUsers = new SQLStmt("SELECT * FROM USERS");
//    
//    public VoltTable[] run() {
//    	System.out.println("Running procedure Get Users");
//    	
//        voltQueueSQL(GetUsers);
//        VoltTable[] results = voltExecuteSQL(true);
//        return results;
//    }   

}
