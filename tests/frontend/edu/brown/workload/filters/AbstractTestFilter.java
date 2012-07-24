package edu.brown.workload.filters;

import java.io.File;

import edu.brown.BaseTestCase;
import edu.brown.utils.ProjectType;
import edu.brown.workload.Workload;

public abstract class AbstractTestFilter extends BaseTestCase {

    protected static final int WORKLOAD_XACT_LIMIT = 1000;
    protected static final int NUM_PARTITIONS = 10;
    protected static final int BASE_PARTITION = 1;
    
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
}
