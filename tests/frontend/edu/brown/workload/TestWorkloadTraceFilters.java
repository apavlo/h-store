package edu.brown.workload;

import java.io.File;

import edu.brown.BaseTestCase;
import edu.brown.utils.ProjectType;
import edu.brown.workload.filters.ProcedureNameFilter;

public class TestWorkloadTraceFilters extends BaseTestCase {

    // Reading the workload takes a long time, so we only want to do it once
    private static AbstractWorkload workload;

    
    @Override
    protected void setUp() throws Exception {
        super.setUp(ProjectType.TM1);
        
        // Super hack! Walk back the directories and find out workload directory
        if (workload == null) {
            File workload_file = this.getWorkloadFile(ProjectType.TM1); 
            workload = new WorkloadTraceFileOutput(catalog);
            
            ((WorkloadTraceFileOutput)workload).load(workload_file.getAbsolutePath(), catalog_db);
            assert(workload.getTransactionCount() > 0) : "No transaction loaded from workload";
        }
    }
    
    /**
     * testProcedureNameFilterWhitelist
     */
    public void testProcedureNameFilterWhitelist() throws Exception {
        
    }
    
    /**
     * testProcedureLimitFilter
     */
    public void testProcedureLimitFilter() throws Exception {
        
    }
    
    
}
