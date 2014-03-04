package edu.brown.benchmark.articles.procedures;

import org.voltdb.SQLStmt;
import org.voltdb.VoltProcedure;
import org.voltdb.VoltTable;

public class AddComment extends VoltProcedure{
//	-- c_id            Comment's ID
//	-- a_id            Article's ID
//	-- u_id            User's ID
//	-- c_text            Actual comment text
	
    public final SQLStmt AddComment = new SQLStmt("INSERT INTO COMMENTS VALUES(?, ?, ?, ?)");
    
    public VoltTable[] run(long c_id, long a_id, long u_id, String c_text) {
        voltQueueSQL(AddComment, c_id, a_id, u_id, c_text);
        return (voltExecuteSQL());
    }   

}
