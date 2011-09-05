package edu.brown.workload;

import java.io.File;
import java.util.HashSet;
import java.util.Set;

import org.junit.Test;
import org.voltdb.catalog.Procedure;

import edu.brown.BaseTestCase;
import edu.brown.benchmark.tm1.procedures.GetTableCounts;
import edu.brown.statistics.Histogram;
import edu.brown.utils.ProjectType;

public class TestWorkloadUtil extends BaseTestCase {

    private static File workload_file;
    private static Set<String> ignore = new HashSet<String>();
    static {
        ignore.add(GetTableCounts.class.getSimpleName());
//        ignore.add(InsertSubscriber.class.getSimpleName());
    }
    
    @Override
    protected void setUp() throws Exception {
        super.setUp(ProjectType.TM1);
        
        if (workload_file == null) {
            workload_file = this.getWorkloadFile(ProjectType.TM1); 
        }
        assertNotNull(workload_file);
    }
    
    /**
     * testGetProcedureHistogram
     */
    @Test
    public void testGetProcedureHistogram() throws Exception {
        Histogram<String> h = WorkloadUtil.getProcedureHistogram(workload_file);
        assertNotNull(h);
        assert(h.getSampleCount() > 0);
        
        for (Procedure catalog_proc : catalog_db.getProcedures()) {
            String proc_name = catalog_proc.getName();
            if (catalog_proc.getSystemproc() || ignore.contains(proc_name)) continue;
            assert(h.get(proc_name, 0) > 0) : "No Entries for " + proc_name;
        } // FOR
        
        System.err.println(h);
    }
    
}
