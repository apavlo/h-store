package edu.brown.benchmark.articles.procedures;

import org.voltdb.ProcInfo;
import org.voltdb.SQLStmt;
import org.voltdb.VoltProcedure;
import org.voltdb.VoltTable;
@ProcInfo(
	    partitionParam = 1,
	    singlePartition = true
	)
public class UpdateUserInfo extends VoltProcedure{
    public final SQLStmt UpdateUser = new SQLStmt("UPDATE USERS SET u_lastname = ? WHERE U_ID = ?");
    
    public VoltTable[] run(String lastname, long u_id) {
        voltQueueSQL(UpdateUser, lastname, u_id);
        return (voltExecuteSQL(true));
    }   

}
