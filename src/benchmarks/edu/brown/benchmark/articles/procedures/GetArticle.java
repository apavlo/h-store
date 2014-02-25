package edu.brown.benchmark.articles.procedures;
import org.voltdb.*;

public class GetArticle extends VoltProcedure{
	 
    public final SQLStmt GetArticle = new SQLStmt("SELECT * FROM ARTICLES WHERE A_ID = ? ");
 
    public VoltTable[] run(long a_id) {
        voltQueueSQL(GetArticle, a_id);
        return (voltExecuteSQL());
    }   

}
