package edu.brown.benchmark.articles.procedures;

import org.apache.log4j.Logger;
import org.voltdb.ProcInfo;
import org.voltdb.SQLStmt;
import org.voltdb.VoltProcedure;
import org.voltdb.VoltTable;
import org.voltdb.exceptions.SerializableException;
import org.voltdb.exceptions.EvictedTupleAccessException;
import edu.brown.benchmark.articles.ArticlesConstants;
import edu.brown.benchmark.articles.ArticlesUtil;
import edu.brown.logging.LoggerUtil;
import edu.brown.logging.LoggerUtil.LoggerBoolean;

@ProcInfo(
    partitionParam = 0,
    singlePartition = true
)
public class AddComment extends VoltProcedure {
    private static final Logger LOG = Logger.getLogger(AddComment.class);
    private static final LoggerBoolean debug = new LoggerBoolean();
    private static final LoggerBoolean trace = new LoggerBoolean();
    static {
        LoggerUtil.attachObserver(LOG, debug, trace);
    }

    public final SQLStmt GetNumComments = new SQLStmt("SELECT A_NUM_COMMENTS FROM ARTICLES WHERE A_ID=?");
    public final SQLStmt AddComment = new SQLStmt("INSERT INTO COMMENTS(C_ID,C_A_ID,C_U_ID,C_TEXT) VALUES(?, ?, ?, ?)");
    public final SQLStmt UpdateArticle = new SQLStmt("UPDATE ARTICLES SET A_NUM_COMMENTS=? WHERE A_ID=?");

    /**
     * 
     * @param a_id - Article's ID
     * @param u_id - User's ID
     * @param c_text - Actual comment text
     * @return
     */
    public VoltTable[] run(long a_id, long u_id, String c_text) {
        VoltTable[] results;
        try {
            voltQueueSQL(GetNumComments, a_id);
            results = voltExecuteSQL();
        } catch (SerializableException ex) {
            LOG.debug("After comments Num comments");
            if (ex instanceof EvictedTupleAccessException) {
                EvictedTupleAccessException exception = (EvictedTupleAccessException) ex;
                if (debug.val) LOG.warn(exception.block_ids[0]);
            }
            throw ex;
        }
        boolean adv = results[0].advanceRow();
        if (adv == false) {
            throw new VoltAbortException("Invalid article id '" + a_id + "'");
        }
        long num_comments = results[0].getLong(0);
        if (num_comments == ArticlesConstants.MAX_COMMENTS_PER_ARTICLE) {
            return results;
        }

        // Compute a new unique comment id
        long c_id = ArticlesUtil.getCommentId(a_id,  num_comments);
        voltQueueSQL(AddComment, c_id, a_id, u_id, c_text);
        voltQueueSQL(UpdateArticle, num_comments + 1, a_id);
        results = voltExecuteSQL(true);
        assert results.length == 2;
        return (results);
    }

}
