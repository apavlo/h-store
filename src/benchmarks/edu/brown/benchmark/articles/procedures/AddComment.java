package edu.brown.benchmark.articles.procedures;

import org.voltdb.ProcInfo;
import org.voltdb.SQLStmt;
import org.voltdb.VoltProcedure;
import org.voltdb.VoltTable;

@ProcInfo(
	    partitionParam = 1,
	    singlePartition = true
	)
public class AddComment extends VoltProcedure{
//	-- c_id            Comment's ID
//	-- a_id            Article's ID
//	-- u_id            User's ID
//	-- c_text            Actual comment text
	
    public final SQLStmt AddComment = new SQLStmt("INSERT INTO COMMENTS(C_ID,A_ID,U_ID,C_TEXT) VALUES(?, ?, ?, ?)");
    public final SQLStmt GetNumComments = new SQLStmt("SELECT A_NUM_COMMENTS FROM ARTICLES WHERE A_ID=?");
    public final SQLStmt UpdateArticle = new SQLStmt("UPDATE ARTICLES SET A_NUM_COMMENTS=? WHERE A_ID=?");

    public VoltTable[] run(long c_id, long a_id, long u_id, String c_text) {
    	System.out.println("Running procedure Add comment "+a_id);
        voltQueueSQL(AddComment, c_id, a_id, u_id, c_text);
        voltExecuteSQL();
        voltQueueSQL(GetNumComments, a_id);
        VoltTable[] results = voltExecuteSQL();
        boolean adv = results[0].advanceRow();
        assert (adv);
        long num_comments = results[0].getLong(0);
        voltQueueSQL(UpdateArticle, num_comments+1, a_id);
        results = voltExecuteSQL(true); 
        assert results.length == 1;
        return results;
    }   

}
