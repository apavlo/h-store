package edu.brown.costmodel;

import java.io.File;
import java.util.List;

import org.apache.log4j.Logger;

import edu.brown.BaseTestCase;

import org.voltdb.VoltProcedure;
import org.voltdb.benchmark.tpcc.procedures.neworder;
import org.voltdb.catalog.ProcParameter;
import org.voltdb.catalog.Procedure;
import org.voltdb.types.ExpressionType;

import edu.brown.catalog.CatalogUtil;
import edu.brown.utils.ProjectType;
import edu.brown.workload.TransactionTrace;
import edu.brown.workload.Workload;
import edu.brown.workload.filters.BasePartitionTxnFilter;
import edu.brown.workload.filters.MultiPartitionTxnFilter;
import edu.brown.workload.filters.ProcParameterArraySizeFilter;
import edu.brown.workload.filters.ProcParameterValueFilter;
import edu.brown.workload.filters.ProcedureLimitFilter;
import edu.brown.workload.filters.ProcedureNameFilter;

public class TestDataPlacementCostModel extends BaseTestCase {
    private static final Logger LOG = Logger.getLogger(TestDataPlacementCostModel.class);

    private static final int WORKLOAD_XACT_LIMIT = 500;
    private static final int PROC_COUNT = 1;
    
    private static final int NUM_HOSTS = 2;
    private static final int NUM_SITES = 2;
    private static final int NUM_PARTITIONS = 4;
    private static final int BASE_PARTITION = 1;
    private static TransactionTrace multip_trace;
    private static Procedure catalog_proc;

    private static final Class<? extends VoltProcedure> TARGET_PROCEDURE = neworder.class;

    // Reading the workload takes a long time, so we only want to do it once
    private static Workload workload;
    
    @Override
    protected void setUp() throws Exception {
        //super.setUp(ProjectType.LOCALITY);
        super.setUp(ProjectType.TPCC);
        LOG.info("number of clusters before: " + catalog_db.getCatalog().getClusters().size());
        LOG.info("number of hosts before: " + this.getCluster().getHosts().size());
        this.initializeCluster(NUM_HOSTS, NUM_SITES, NUM_PARTITIONS);
        LOG.info("number of hosts after: " + this.getCluster().getHosts().size());
        LOG.info("number of clusters after: " + catalog_db.getCatalog().getClusters().size());
        //this.addPartitions(NUM_PARTITIONS);
        // Super hack! Walk back the directories and find out workload directory
        if (workload == null) {
            //File workload_file = this.getWorkloadFile(ProjectType.LOCALITY); 
            File file = this.getWorkloadFile(ProjectType.TPCC, "100w.large"); 
            workload = new Workload(catalog);
            catalog_proc = this.getProcedure(TARGET_PROCEDURE);
            
//            ProcedureLimitFilter filter = new ProcedureLimitFilter(WORKLOAD_COUNT);
//            workload.load(workload_file.getAbsolutePath(), catalog_db, filter);
            
            // Check out this beauty:
            // (1) Filter by procedure name
            // (2) Filter on partitions that start on our BASE_PARTITION
            // (3) Filter to only include multi-partition txns
            // (4) Another limit to stop after allowing ### txns
            // Where is your god now???
            Workload.Filter filter = new ProcedureNameFilter()
                    .include(TARGET_PROCEDURE.getSimpleName())
                    .attach(new ProcParameterValueFilter().include(1, new Long(5))) // D_ID
                    .attach(new ProcParameterArraySizeFilter(CatalogUtil.getArrayProcParameters(catalog_proc).get(0), 10, ExpressionType.COMPARE_EQUAL))
                    .attach(new BasePartitionTxnFilter(p_estimator, BASE_PARTITION))
                    .attach(new MultiPartitionTxnFilter(p_estimator))
                    .attach(new ProcedureLimitFilter(WORKLOAD_XACT_LIMIT));
            workload.load(file.getAbsolutePath(), catalog_db, filter);
            
//            // Workload Filter
//            ProcedureNameFilter name_filter = new ProcedureNameFilter();
//            long total = 0;
//            for (String proc_name : TARGET_PROCEDURES) {
//                name_filter.include(proc_name, PROC_COUNT);
//                total += PROC_COUNT;
//            }
//            ((Workload)workload).load(workload_file.getAbsolutePath(), catalog_db, name_filter);

            
            for (TransactionTrace xact : workload.getTransactions()) {
                Object ol_supply_w_ids[] = (Object[])xact.getParam(5);
                assert(ol_supply_w_ids != null);
                boolean same_partition = true;
                for (Object i : ol_supply_w_ids) {
                    Integer partition = p_estimator.getHasher().hash(Integer.valueOf(i.toString()));
                    // why compare against base partition (1) every time?
                    same_partition = same_partition && (partition == BASE_PARTITION);
                    if (same_partition == false && multip_trace == null) {
                        LOG.info("multi transaction");
                    }
                } // FOR
                if (same_partition == false && multip_trace == null) {
                    multip_trace = xact;
                }
            } // FOR
            
            
            
            
            
//            assertEquals(total, workload.getTransactionCount());
//            assertEquals(TARGET_PROCEDURES.length, workload.getProcedureHistogram().getValueCount());
            // System.err.println(workload.getProcedureHistogram());
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
