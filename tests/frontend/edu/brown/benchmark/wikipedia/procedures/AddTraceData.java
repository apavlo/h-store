package edu.brown.benchmark.wikipedia.procedures;

import java.util.Random;

import org.apache.log4j.Logger;
import org.voltdb.SQLStmt;
import org.voltdb.VoltProcedure;
import org.voltdb.VoltTable;

import edu.brown.benchmark.wikipedia.WikipediaConstants;
import edu.brown.rand.RandomDistribution.Flat;
import edu.brown.rand.RandomDistribution.Zipf;


public class AddTraceData extends VoltProcedure {
    private static final Logger LOG = Logger.getLogger(AddTraceData.class);
    private Random randGenerator = new Random();
    
    public SQLStmt getTitle = new SQLStmt (
            " SELECT page_namespace,page_title " +
            " FROM " + WikipediaConstants.TABLENAME_PAGE + 
            " WHERE page_id = ?");
    
    public SQLStmt insertGenTrace = new SQLStmt (
            "INSERT INTO " + WikipediaConstants.GET_TRACE_COLS + " (" +
            " USER_ID, NAMESPACE, TITLE " + 
            " ) VALUES ( " + 
            " ?, ?, ? " +
            " )"
            ); 
    
    
    public long run () {
        
        double m_scalefactor = 1.0;
        int num_users = (int) Math.round(WikipediaConstants.USERS * m_scalefactor);
        int num_pages = (int) Math.round(WikipediaConstants.PAGES * m_scalefactor);
        Flat z_users = new Flat(this.randGenerator, 1, num_users);
        Zipf z_pages = new Zipf(this.randGenerator, 1, num_pages, WikipediaConstants.USER_ID_SIGMA);
        
        //int batch_size = 5;
        //for (int i = 0, cnt = (b.getTraceSize() * 1000); i < cnt; i++) {
        for (int i = 0, cnt = (WikipediaConstants.TRACE_DATA_SCALE); i < cnt; i++) {
            int user_id = -1;
            
            // Check whether this should be an anonymous update
            if (this.randGenerator.nextInt(100) < WikipediaConstants.ANONYMOUS_PAGE_UPDATE_PROB) {
                user_id = WikipediaConstants.ANONYMOUS_USER_ID;
            }
            // Otherwise figure out what user is updating this page
            else {
                user_id = z_users.nextInt();
            }
            assert(user_id != -1);
            
            // Figure out what page they're going to update
            int page_id = z_pages.nextInt();
            voltQueueSQL(getTitle, page_id);
            VoltTable res[] = voltExecuteSQL();
            
            //Pair<Integer, String> p = titles.get(page_id);
            voltQueueSQL(insertGenTrace, user_id,res[0].getLong(0),res[0].getString(1));
            
            voltExecuteSQL();
        } // FOR
        
        voltExecuteSQL();
        
        return 1;
    }
    
}
