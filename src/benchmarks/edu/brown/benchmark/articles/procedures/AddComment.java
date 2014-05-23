package edu.brown.benchmark.articles.procedures;

import org.apache.log4j.Logger;
import org.voltdb.ProcInfo;
import org.voltdb.SQLStmt;
import org.voltdb.VoltProcedure;
import org.voltdb.VoltTable;
import org.voltdb.exceptions.SerializableException;
import org.voltdb.exceptions.EvictedTupleAccessException;
import edu.brown.benchmark.articles.ArticlesConstants;
import edu.brown.logging.LoggerUtil;
import edu.brown.logging.LoggerUtil.LoggerBoolean;

@ProcInfo(
	    partitionParam = 0,
	    singlePartition = true
	)
public class AddComment extends VoltProcedure{
//	-- c_id            Comment's ID
//	-- a_id            Article's ID
//	-- u_id            User's ID
//	-- c_text            Actual comment text
    public static final Logger LOG = Logger.getLogger(AddComment.class);
    private static final LoggerBoolean debug = new LoggerBoolean();
    private static final LoggerBoolean trace = new LoggerBoolean();
    static {
        LoggerUtil.setupLogging();
        LoggerUtil.attachObserver(LOG, debug, trace);
    }

    
    public final SQLStmt GetNumComments = new SQLStmt("SELECT A_NUM_COMMENTS FROM ARTICLES WHERE A_ID=?");
    public final SQLStmt AddComment = new SQLStmt("INSERT INTO COMMENTS(C_ID,C_A_ID,C_U_ID,C_TEXT) VALUES(?, ?, ?, ?)");
    public final SQLStmt UpdateArticle = new SQLStmt("UPDATE ARTICLES SET A_NUM_COMMENTS=? WHERE A_ID=?");

    public VoltTable[] run(long a_id, long u_id, String c_text) {
    	VoltTable[] results;
    	try {
	        voltQueueSQL(GetNumComments, a_id);
	        results = voltExecuteSQL();
    	} catch (SerializableException ex) {
    		LOG.info("After comments Num comments");
    		System.out.println(ex);
		if(ex instanceof EvictedTupleAccessException){
                       EvictedTupleAccessException exception = (EvictedTupleAccessException) ex;
                       LOG.info(exception.block_ids[0]);
			System.out.println(a_id);
               }
    		throw ex;
    	}
        boolean adv = results[0].advanceRow();
        //assert (adv);
        long num_comments = results[0].getLong(0);
        if(num_comments == ArticlesConstants.MAX_COMMENTS_PER_ARTICLE){
        	return results;
        }

        long c_id = a_id*ArticlesConstants.MAX_COMMENTS_PER_ARTICLE+num_comments;
        try{
        voltQueueSQL(AddComment, c_id, a_id, u_id, c_text);
        voltExecuteSQL();
        } catch (SerializableException ex){
        	LOG.info("After add comment");
        	throw ex;
        }
        
        try{
            voltQueueSQL(UpdateArticle, num_comments+1, a_id);
            results = voltExecuteSQL(true);         	
        }catch (SerializableException ex){
        	LOG.info("After update article");
        	throw ex;
        }
        assert results.length == 1;
        return results;
    }   

}
