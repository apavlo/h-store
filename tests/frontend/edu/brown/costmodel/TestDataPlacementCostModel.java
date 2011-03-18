package edu.brown.costmodel;

import java.io.File;
import java.util.List;

import org.apache.log4j.Logger;
import org.hsqldb.Database;

import edu.brown.BaseTestCase;

import org.voltdb.VoltProcedure;
import org.voltdb.benchmark.tpcc.procedures.neworder;
import org.voltdb.catalog.CatalogType;
import org.voltdb.catalog.Cluster;
import org.voltdb.catalog.ProcParameter;
import org.voltdb.catalog.Procedure;
import org.voltdb.types.ExpressionType;

import sun.security.acl.WorldGroupImpl;

import edu.brown.catalog.CatalogUtil;
import edu.brown.catalog.FixCatalog;
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

    private static final int WORKLOAD_XACT_LIMIT = 50;
    private static final int PROC_COUNT = 1;
    
    private static final int NUM_HOSTS = 100;
    private static final int NUM_SITES = 2;
    private static final int NUM_PARTITIONS = 1;
    private static final int BASE_PARTITION = 1;
    private static Procedure catalog_proc;

    private static final Class<? extends VoltProcedure> TARGET_PROCEDURE = neworder.class;

    // Reading the workload takes a long time, so we only want to do it once
    private static Workload workload;
    
    @Override
    protected void setUp() throws Exception {
        //super.setUp(ProjectType.LOCALITY);
        super.setUp(ProjectType.TPCC);
        
        // set the cluster configuration
        this.initializeCluster(100, 1, 1);
        if (workload == null) {
            // File workload_file = this.getWorkloadFile(ProjectType.LOCALITY);
            File file = this.getWorkloadFile(ProjectType.TPCC, "100w.large");
            workload = new Workload(catalog);
            catalog_proc = this.getProcedure(TARGET_PROCEDURE);

            // Check out this beauty:
            // (1) Filter by procedure name
            // (2) Filter on partitions that start on our BASE_PARTITION
            // (3) Filter to only include multi-partition txns
            // (4) Another limit to stop after allowing ### txns
            // Where is your god now???
            LOG.info("filter starting to apply filter");
            Workload.Filter filter = new ProcedureNameFilter().include(TARGET_PROCEDURE.getSimpleName()).attach(new BasePartitionTxnFilter(p_estimator, BASE_PARTITION)).attach(
                    new MultiPartitionTxnFilter(p_estimator)).attach(new ProcedureLimitFilter(WORKLOAD_XACT_LIMIT));
            LOG.info("filter: " + filter + " catalogdb: " + (catalog_db));
            workload.load(file.getAbsolutePath(), catalog_db, filter);
        }
        assert (workload.getTransactionCount() > 0);

        //this.addPartitions(NUM_PARTITIONS);
        // Super hack! Walk back the directories and find out workload directory
    }

    /**
     * testEstimateCost
     */
    public void testEstimateCostMultiPartition() throws Exception {
//        LOG.info("transactions in workload: " + workload.getTransactionCount());
//        
//        LOG.info("AFTER HOSTS: " + CatalogUtil.getNumberOfHosts(catalog_db));
//        LOG.info("AFTER SITES: " + CatalogUtil.getNumberOfSites(catalog_db));
//        LOG.info("AFTER PARTITIONS: " + CatalogUtil.getNumberOfPartitions(catalog_db));
//        
//        LOG.info("num of transactions: " + workload.getTransactionCount());
//        System.err.println(workload.getTransactions().get(0).debug(catalog_db));
//        
//        // Now calculate cost of touching these partitions
//        TransactionTrace txn_trace = workload.getTransactions().get(0);
//        LOG.info("transaction trace: " + txn_trace);
//        assertNotNull(txn_trace);
//        DataPlacementCostModel cost_model = new DataPlacementCostModel(catalog_db);
//        cost_model.prepare(catalog_db);
//        double cost = cost_model.estimateTransactionCost(catalog_db, txn_trace);
//        LOG.info("total cost for transaction " + txn_trace.getCatalogItemName() + " cost: " + cost);
//        assert(cost > 0);
    }
    
    /**
     * testTransformTransactionTraces
     */
    public void testTransformTransactionTraces() throws Exception {
        TransformTransactionTraces.transform(workload.getTraces(catalog_proc), p_estimator, catalog_db);
    }
}
