package edu.brown.benchmark.articles.procedures;
import org.voltdb.*;

@ProcInfo(
	    partitionParam = 0,
	    singlePartition = true
	)
public class GetArticle extends VoltProcedure{
	 
    public final SQLStmt GetArticle = new SQLStmt("SELECT * FROM ARTICLES WHERE A_ID = ? ");
    public final SQLStmt GetComments = new SQLStmt("SELECT * FROM COMMENTS WHERE C_A_ID = ? ");
//    public final SQLStmt GetUser = new SQLStmt("SELECT * FROM USERS JOIN COMMENTS ON USERS.U_ID = COMMENTS.U_ID WHERE A_ID = ? ");

    public VoltTable[] run(long a_id) {
    	System.out.println("Running procedure Get article "+a_id);
    	
    	voltQueueSQL(GetArticle, a_id);
        VoltTable[] article = voltExecuteSQL();
        voltQueueSQL(GetComments, a_id);
        VoltTable[] comments = voltExecuteSQL(true);
        
//        voltQueueSQL(GetUser, a_id);
        return article;
    }   

}
