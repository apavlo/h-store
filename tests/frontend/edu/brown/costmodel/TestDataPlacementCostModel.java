package edu.brown.costmodel;

import java.io.File;

import edu.brown.BaseTestCase;
import edu.brown.benchmark.tm1.procedures.DeleteCallForwarding;
import edu.brown.benchmark.tm1.procedures.GetAccessData;
import edu.brown.benchmark.tm1.procedures.GetNewDestination;
import edu.brown.utils.ProjectType;
import edu.brown.workload.TransactionTrace;
import edu.brown.workload.Workload;
import edu.brown.workload.filters.ProcedureNameFilter;

public class TestDataPlacementCostModel extends BaseTestCase {

    private static final int PROC_COUNT = 3;
    private static final String TARGET_PROCEDURES[] = {
        DeleteCallForwarding.class.getSimpleName(),
        GetAccessData.class.getSimpleName(),
        GetNewDestination.class.getSimpleName(),
    };
    private static final boolean TARGET_PROCEDURES_SINGLEPARTITION_DEFAULT[] = {
        false,  // DeleteCallForwarding
        true,   // GetAccessData
        true,   // GetNewDestination
    };
    
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
    }

    /**
     * testEstimateCost
     */
    public void testEstimateCost() throws Exception {
        for (TransactionTrace xact : workload.getTransactions()) {
            assertNotNull(xact);
            DataPlacementCostModel cost_model = new DataPlacementCostModel(catalog_db);
            cost_model.estimateTransactionCost(catalog_db, xact);
        } // FOR
        // System.err.println(xact_trace.debug(catalog_db));
    }
}
