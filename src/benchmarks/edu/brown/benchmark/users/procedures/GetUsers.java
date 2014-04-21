package edu.brown.benchmark.users.procedures;

import org.voltdb.ProcInfo;
import org.voltdb.SQLStmt;
import org.voltdb.VoltProcedure;
import org.voltdb.VoltTable;

import edu.brown.benchmark.articles.ArticlesConstants;

@ProcInfo(
	    singlePartition = false
	)
public class GetUsers extends VoltProcedure{
//	-- c_id            Comment's ID
//	-- a_id            Article's ID
//	-- u_id            User's ID
//	-- c_text            Actual comment text
	
    
    public final SQLStmt GetUsers = new SQLStmt("SELECT * FROM USERS");
    
    public VoltTable[] run() {
    	System.out.println("Running procedure Get Users");
    	
        voltQueueSQL(GetUsers);
        VoltTable[] results = voltExecuteSQL(true);
        return results;
    }   

}
