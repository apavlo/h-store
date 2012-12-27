package edu.brown.workload;

import java.io.File;

import org.junit.Test;

import edu.brown.BaseTestCase;
import edu.brown.statistics.Histogram;
import edu.brown.statistics.ObjectHistogram;
import edu.brown.utils.ProjectType;
import edu.brown.workload.Workload;
import edu.brown.workload.filters.ProcedureLimitFilter;

public class TestWorkloadFile extends BaseTestCase {

    protected static final int WORKLOAD_XACT_LIMIT = 5000;
    protected static final int NUM_PARTITIONS = 10;
    protected static final int BASE_PARTITION = 0;
    protected static final int NUM_INTERVALS  = 30;
    
    // Reading the workload takes a long time, so we only want to do it once
    protected static Workload workload;
    protected static File workload_file;
    
    @Override
    protected void setUp() throws Exception {
        super.setUp(ProjectType.TPCC);
        this.addPartitions(NUM_PARTITIONS);
        
        if (workload == null) {
            workload_file = this.getWorkloadFile(ProjectType.TPCC); 
            workload = new Workload(catalog);
            
            ((Workload)workload).load(workload_file, catalog_db, new ProcedureLimitFilter(WORKLOAD_XACT_LIMIT));
            assert(workload.getTransactionCount() > 0) : "No transaction loaded from workload";
            assertEquals(WORKLOAD_XACT_LIMIT, workload.getTransactionCount());
        }
        assertNotNull(workload_file);
    }
    
    /**
     * testGetTimeInterval
     */
    @Test
    public void testGetTimeInterval() throws Exception {
        Histogram<Integer> h = new ObjectHistogram<Integer>();
        for (TransactionTrace txn_trace : workload.getTransactions()) {
            assert(txn_trace.getQueryCount() > 0) : txn_trace.debug(catalog_db);
            int interval = workload.getTimeInterval(txn_trace, NUM_INTERVALS);
            assert(interval >= 0) : "Invalid Interval: " + interval;
            assert(interval < NUM_INTERVALS) : "Invalid Interval: " + interval;
            h.put(interval);
        } // FOR
        
        System.err.println(h);
        for (int interval = 0; interval < NUM_INTERVALS; interval++) {
            Long count = h.get(interval);
            assertNotNull("No results for Interval #" + interval, count);
        } // FOR

        // Make sure the last interval doesn't only have one entry!
        assertNotSame("Only entry in last interval!", 1l, h.get(NUM_INTERVALS - 1).longValue());
    }
}
