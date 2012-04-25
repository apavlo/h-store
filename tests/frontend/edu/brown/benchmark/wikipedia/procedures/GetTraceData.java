package edu.brown.benchmark.wikipedia.procedures;


import org.apache.log4j.Logger;
import org.voltdb.SQLStmt;
import org.voltdb.VoltProcedure;
import org.voltdb.VoltTable;

import edu.brown.benchmark.wikipedia.WikipediaConstants;



public class GetTraceData extends VoltProcedure {
    
    private static final Logger LOG = Logger.getLogger(GetTraceData.class);
    
    public SQLStmt CountTraceData = new SQLStmt (
            " SELECT * FROM " + WikipediaConstants.GET_TRACE_COLS + 
            " WHERE user_id = ? "
            );
    
    
    public VoltTable run(int user_id) {
        
        voltQueueSQL(CountTraceData, user_id);
        
        VoltTable result[] = voltExecuteSQL();
        
        return result[0];
    }


}
