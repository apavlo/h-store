package edu.brown.benchmark.articles.procedures;

import org.voltdb.SQLStmt;
import org.voltdb.VoltProcedure;
import org.voltdb.VoltTable;

public class GetArticles extends VoltProcedure{
    public final SQLStmt GetArticles = new SQLStmt("SELECT * FROM ARTICLES where a_id > ?");
    
    public VoltTable[] run(long a_id) {
        voltQueueSQL(GetArticles, a_id);
        return (voltExecuteSQL());
    }   

}
