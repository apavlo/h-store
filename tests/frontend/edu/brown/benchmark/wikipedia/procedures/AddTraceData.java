package edu.brown.benchmark.wikipedia.procedures;

import java.io.File;
import java.io.PrintStream;
import java.util.List;
import java.util.Random;

import org.voltdb.SQLStmt;
import org.voltdb.VoltTable;
import org.apache.log4j.Logger;
import org.voltdb.VoltProcedure;
import org.voltdb.utils.Pair;

import com.oltpbenchmark.benchmarks.wikipedia.util.TransactionSelector;

import edu.brown.benchmark.wikipedia.WikipediaConstants;
import edu.brown.rand.RandomDistribution.Flat;
import edu.brown.rand.RandomDistribution.Zipf;


public class AddTraceData extends VoltProcedure {
    private static final Logger LOG = Logger.getLogger(AddTraceData.class);
    private Random randGenerator = new Random();
    
    public SQLStmt insertGenTrace = new SQLStmt (
            "INSERT INTO " + WikipediaConstants.GET_TRACE_COLS + " (" +
            " USER_ID, NAMESPACE, TITLE " + 
            " ) VALUES ( " + 
            " ?, ?, ? " +
            " )"
            ); 
    
    
    public long run (int num_users, int num_pages, List<Pair<Integer, String>> titles) {
        
        Flat z_users = new Flat(this.randGenerator, 1, num_users);
        Zipf z_pages = new Zipf(this.randGenerator, 1, num_pages, WikipediaConstants.USER_ID_SIGMA);
        
        assert(num_pages == titles.size());
        
        int batch_size = 5;
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
            Pair<Integer, String> p = titles.get(page_id);
            assert(p != null);
            
            voltQueueSQL(insertGenTrace, user_id,p.getFirst(),p.getSecond());
            
            if (i % batch_size == 0)
                voltExecuteSQL();
        } // FOR
        
        voltExecuteSQL();
        
        return 1;
    }
    
}
