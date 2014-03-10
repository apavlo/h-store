package edu.brown.benchmark.articles.procedures;

import org.voltdb.ProcInfo;
import org.voltdb.SQLStmt;
import org.voltdb.VoltProcedure;
import org.voltdb.VoltTable;
@ProcInfo(
	    partitionInfo = "ARTICLES.A_ID: 0",
	    singlePartition = true
	)
public class AddComment extends VoltProcedure{
//	-- c_id            Comment's ID
//	-- a_id            Article's ID
//	-- u_id            User's ID
//	-- c_text            Actual comment text
	
    public final SQLStmt AddComment = new SQLStmt("INSERT INTO COMMENTS(C_ID,A_ID,U_ID,C_TEXT) VALUES(?, ?, ?, ?)");
//    public final SQLStmt UpdateArticle = new SQLStmt("UPDATE ARTICLES SET A_NUM_COMMENTS=A_NUM_COMMENTS+1 where A_ID=?");

    public VoltTable[] run(long c_id, long a_id, long u_id, String c_text) {
    	System.out.println("Running procedure Add comment "+a_id);
        voltQueueSQL(AddComment, c_id, a_id, u_id, c_text);
        //voltQueueSQL(UpdateArticle, a_id);
        return (voltExecuteSQL(true));
    }   

}
