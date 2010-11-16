package edu.brown.workload;

import java.io.File;

import org.junit.Test;

import edu.brown.BaseTestCase;
import edu.brown.statistics.Histogram;
import edu.brown.utils.ProjectType;

public class TestWorkloadUtil extends BaseTestCase {

    // Reading the workload takes a long time, so we only want to do it once
    protected static Workload workload;
    protected static File workload_file;
    
    @Override
    protected void setUp() throws Exception {
        super.setUp(ProjectType.TM1);
        
        if (workload_file == null) {
            workload_file = this.getWorkloadFile(ProjectType.TPCC); 
        }
        assertNotNull(workload_file);
    }
    
    /**
     * testGetProcedureHistogram
     */
    @Test
    public void testGetProcedureHistogram() throws Exception {
        Histogram h = WorkloadUtil.getProcedureHistogram(workload_file);
        assertNotNull(h);
        assert(h.getSampleCount() > 0);
    }
    
}
