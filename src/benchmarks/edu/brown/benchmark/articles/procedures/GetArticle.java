package edu.brown.benchmark.articles.procedures;
import org.apache.log4j.Logger;
import org.voltdb.*;
import org.voltdb.exceptions.EvictedTupleAccessException;
import org.voltdb.exceptions.SerializableException;

import edu.brown.logging.LoggerUtil;
import edu.brown.logging.LoggerUtil.LoggerBoolean;

@ProcInfo(
	    partitionParam = 0,
	    singlePartition = true
	)
public class GetArticle extends VoltProcedure{
    public static final Logger LOG = Logger.getLogger(AddComment.class);
    private static final LoggerBoolean debug = new LoggerBoolean();
    private static final LoggerBoolean trace = new LoggerBoolean();
    static {
        LoggerUtil.setupLogging();
        LoggerUtil.attachObserver(LOG, debug, trace);
    }

    public final SQLStmt GetArticle = new SQLStmt("SELECT * FROM ARTICLES WHERE A_ID = ? ");
//public final SQLStmt GetComments = new SQLStmt("SELECT * FROM COMMENTS WHERE C_A_ID = ? ");
//    public final SQLStmt GetUser = new SQLStmt("SELECT * FROM USERS JOIN COMMENTS ON USERS.U_ID = COMMENTS.U_ID WHERE A_ID = ? ");

    public VoltTable[] run(long a_id) {
    	try {
    	voltQueueSQL(GetArticle, a_id);
        return voltExecuteSQL(true);
    	} catch (SerializableException ex) {
		    if(ex instanceof EvictedTupleAccessException){
                       EvictedTupleAccessException exception = (EvictedTupleAccessException) ex;
                if (debug.val) {
                    LOG.info(exception.block_ids[0]);
			        System.out.println(a_id);
                }
            } else {
	            LOG.info("After get article");
    		    System.out.println(ex);
            }
    		throw ex;
    	}
/*    	try{
    		voltQueueSQL(GetComments, a_id);
            return voltExecuteSQL(true);	
    	} catch (SerializableException ex) {
    		LOG.info("After get comments");
    		System.out.println(ex);
		if(ex instanceof EvictedTupleAccessException){
                       EvictedTupleAccessException exception = (EvictedTupleAccessException) ex;
                       LOG.info(exception.block_ids[0]);
			System.out.println(a_id);
               }
    		throw ex;
    	}
*/       
    }   

}
