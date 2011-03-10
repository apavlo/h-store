package edu.brown.costmodel;

import java.io.File;

import org.apache.log4j.Logger;

import edu.brown.BaseTestCase;
import edu.brown.benchmark.tm1.procedures.DeleteCallForwarding;
import edu.brown.benchmark.tm1.procedures.GetAccessData;
import edu.brown.benchmark.tm1.procedures.GetNewDestination;
import edu.brown.utils.ProjectType;
import edu.brown.workload.TransactionTrace;
import edu.brown.workload.Workload;
import edu.brown.workload.filters.ProcedureLimitFilter;
import edu.brown.workload.filters.ProcedureNameFilter;

public class TestDataPlacementCostModel extends BaseTestCase {
    private static final Logger LOG = Logger.getLogger(TestDataPlacementCostModel.class);

    private static final int WORKLOAD_COUNT = 1000;
    
    private static final int NUM_HOSTS = 2;
    private static final int NUM_SITES = 2;
    private static final int NUM_PARTITIONS = 2;
    
    // Reading the workload takes a long time, so we only want to do it once
    private static Workload workload;
    
    @Override
    protected void setUp() throws Exception {
        super.setUp(ProjectType.LOCALITY);
        System.out.println("number of clusters before: " + catalog_db.getCatalog().getClusters().size());
        System.out.println("number of hosts before: " + this.getCluster().getHosts().size());
        this.initializeCluster(NUM_HOSTS, NUM_SITES, NUM_PARTITIONS);
        System.out.println("number of hosts after: " + this.getCluster().getHosts().size());
        System.out.println("number of clusters after: " + catalog_db.getCatalog().getClusters().size());
        //this.addPartitions(NUM_PARTITIONS);
        // Super hack! Walk back the directories and find out workload directory
        if (workload == null) {
            File workload_file = this.getWorkloadFile(ProjectType.LOCALITY); 
            workload = new Workload(catalog);
            
            ProcedureLimitFilter filter = new ProcedureLimitFilter(WORKLOAD_COUNT);
            workload.load(workload_file.getAbsolutePath(), catalog_db, filter);
            
//            // Workload Filter
//            ProcedureNameFilter filter = new ProcedureNameFilter();
//            long total = 0;
//            for (String proc_name : TARGET_PROCEDURES) {
//                filter.include(proc_name, PROC_COUNT);
//                total += PROC_COUNT;
//            }
//              ((Workload)workload).load(workload_file.getAbsolutePath(), catalog_db);
//            assertEquals(total, workload.getTransactionCount());
//            assertEquals(TARGET_PROCEDURES.length, workload.getProcedureHistogram().getValueCount());
//            // System.err.println(workload.getProcedureHistogram());
        }
        assert(workload.getTransactionCount() > 0);
    }

    /**
     * testEstimateCost
     */
    public void testEstimateCost() throws Exception {
        // Now calculate cost of touching these partitions 
        DataPlacementCostModel cost_model = new DataPlacementCostModel(catalog_db);
        LOG.info("total cost: " + cost_model.estimateCost(catalog_db, workload));
    }
}
