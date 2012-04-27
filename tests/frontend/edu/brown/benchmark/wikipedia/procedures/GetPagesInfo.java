package edu.brown.benchmark.wikipedia.procedures;

import org.voltdb.SQLStmt;
import org.voltdb.VoltProcedure;
import org.voltdb.VoltTable;

import edu.brown.benchmark.wikipedia.WikipediaConstants;


public class GetPagesInfo extends VoltProcedure {
    
    public SQLStmt getAllPages = new SQLStmt (
            " SELECT page_id, page_namespace, page_title " +
            " FROM " + WikipediaConstants.TABLENAME_PAGE);
    
    public VoltTable[] run() {
        voltQueueSQL(getAllPages);
        return voltExecuteSQL();
    }
    
}
