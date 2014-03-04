package edu.brown.benchmark.articles.procedures;

import org.voltdb.SQLStmt;
import org.voltdb.VoltProcedure;
import org.voltdb.VoltTable;

public class GetArticles extends VoltProcedure{
    public final SQLStmt GetArticles = new SQLStmt("SELECT * FROM ARTICLES");
    
    public VoltTable[] run() {
        voltQueueSQL(GetArticles);
        return (voltExecuteSQL());
    }   

}
