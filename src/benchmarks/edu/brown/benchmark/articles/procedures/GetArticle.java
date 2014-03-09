package edu.brown.benchmark.articles.procedures;
import org.voltdb.*;

public class GetArticle extends VoltProcedure{
	 
    public final SQLStmt GetArticle = new SQLStmt("SELECT * FROM ARTICLES WHERE A_ID = ? ");
    public final SQLStmt GetComments = new SQLStmt("SELECT * FROM COMMENTS WHERE A_ID = ? ");
//    public final SQLStmt GetUser = new SQLStmt("SELECT * FROM USERS JOIN COMMENTS ON USERS.U_ID = COMMENTS.U_ID WHERE A_ID = ? ");

    public VoltTable[] run(long a_id) {
        voltQueueSQL(GetArticle, a_id);
        voltQueueSQL(GetComments, a_id);
//        voltQueueSQL(GetUser, a_id);
        return (voltExecuteSQL());
    }   

}
