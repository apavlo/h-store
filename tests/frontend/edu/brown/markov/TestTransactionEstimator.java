package edu.brown.markov;

import java.io.File;
import java.util.*;

import org.junit.Test;
import org.voltdb.VoltProcedure;
import org.voltdb.benchmark.tpcc.procedures.neworder;
import org.voltdb.catalog.*;

import edu.brown.BaseTestCase;
import edu.brown.catalog.CatalogUtil;
import edu.brown.correlations.ParameterCorrelations;
import edu.brown.markov.TransactionEstimator.Estimate;
import edu.brown.utils.*;
import edu.brown.workload.AbstractWorkload;
import edu.brown.workload.TransactionTrace;
import edu.brown.workload.WorkloadTraceFileOutput;
import edu.brown.workload.filters.BasePartitionTxnFilter;
import edu.brown.workload.filters.ProcedureNameFilter;

/**
 * 
 * @author pavlo
 *
 */
public class TestTransactionEstimator extends BaseTestCase {

    public static final Random rand = new Random();
    
    private static final int WORKLOAD_XACT_LIMIT = 10000;
    private static final int BASE_PARTITION = 1;
    private static final int NUM_PARTITIONS = 10;
    private static List<Integer> ALL_PARTITIONS;
    private static final Class<? extends VoltProcedure> TARGET_PROCEDURE = neworder.class;
    private static int XACT_ID = 1000;

    private static AbstractWorkload workload;
    private static MarkovGraphsContainer markovs;
    private static ParameterCorrelations correlations;
    private static TransactionTrace singlep_trace;
    private static TransactionTrace multip_trace;
    private static final Set<Integer> multip_partitions = new HashSet<Integer>();
    private static final List<Vertex> multip_path = new ArrayList<Vertex>();
    
    private TransactionEstimator t_estimator;
    private EstimationThresholds thresholds;
    private Procedure catalog_proc;
    
    @Override
    protected void setUp() throws Exception {
        super.setUp(ProjectType.TPCC);
        this.addPartitions(NUM_PARTITIONS);
        ALL_PARTITIONS = CatalogUtil.getAllPartitionIds(catalog_db);
        
        this.catalog_proc = this.getProcedure(TARGET_PROCEDURE);
        
        if (markovs == null) {
            File file = this.getCorrelationsFile(ProjectType.TPCC);
            correlations = new ParameterCorrelations();
            correlations.load(file.getAbsolutePath(), catalog_db);
            
            file = this.getWorkloadFile(ProjectType.TPCC);
            workload = new WorkloadTraceFileOutput(catalog);
            ProcedureNameFilter filter = new ProcedureNameFilter();
            filter.include(TARGET_PROCEDURE.getSimpleName(), WORKLOAD_XACT_LIMIT);
            
            // Custom filter that only grabs neworders that go to our BASE_PARTITION
            filter.attach(new BasePartitionTxnFilter(p_estimator, BASE_PARTITION));
            ((WorkloadTraceFileOutput) workload).load(file.getAbsolutePath(), catalog_db, filter);
            
            // Generate MarkovGraphs
            markovs = MarkovUtil.createGraphs(catalog_db, workload, p_estimator);
            assertNotNull(markovs);
            
            // Find a single-partition and multi-partition trace
            multip_partitions.add(BASE_PARTITION);
            for (TransactionTrace xact : workload.getTransactions()) {
                Object ol_supply_w_ids[] = (Object[])xact.getParam(5);
                assert(ol_supply_w_ids != null);
                boolean same_partition = true;
                for (Object i : ol_supply_w_ids) {
                    Integer partition = p_estimator.getHasher().hash(Integer.valueOf(i.toString()));
                    same_partition = same_partition && (partition == BASE_PARTITION);
                    if (same_partition == false && multip_trace == null) {
                        multip_partitions.add(partition);
                    }
                } // FOR
                if (same_partition && singlep_trace == null) singlep_trace = xact;
                if (same_partition == false && multip_trace == null) {
                    multip_trace = xact;
                    multip_path.addAll(markovs.get(BASE_PARTITION, this.catalog_proc).processTransaction(xact, p_estimator));
                }
                if (singlep_trace != null && multip_trace != null) break;
            } // FOR
        }
        assertNotNull(singlep_trace);
        assertNotNull(multip_trace);
        assert(multip_partitions.size() > 1);
        assertFalse(multip_path.isEmpty());
        
        // Setup
        this.t_estimator = new TransactionEstimator(BASE_PARTITION, p_estimator, correlations);
        this.t_estimator.addMarkovGraphs(markovs.get(BASE_PARTITION));
        this.thresholds = new EstimationThresholds();
    }
    
    /**
     * testStartTransaction
     */
    @Test
    public void testStartTransaction() {
        Estimate est = t_estimator.startTransaction(XACT_ID++, this.catalog_proc, singlep_trace.getParams());
        assertNotNull(est);
        System.err.println(est.toString());
        
        assert(est.isSinglePartition(this.thresholds));
        assertFalse(est.isUserAbort(this.thresholds));
        
        /* FIXME
        for (Integer partition : ALL_PARTITIONS) {
            if (partition == BASE_PARTITION) {
                assertFalse("isFinishedPartition(" + partition + ")", est.isFinishedPartition(thresholds, partition));
                assertTrue("isWritePartition(" + partition + ")", est.isWritePartition(thresholds, partition) == true);
                assertTrue("isTargetPartition(" + partition + ")", est.isTargetPartition(thresholds, partition) == true);
            } else {
                assertTrue("isFinishedPartition(" + partition + ")", est.isFinishedPartition(thresholds, partition));
                assertFalse("isWritePartition(" + partition + ")", est.isWritePartition(thresholds, partition) == true);
                assertFalse("isTargetPartition(" + partition + ")", est.isTargetPartition(thresholds, partition) == true);
            }
        } // FOR
        */
    }
    
    @Test
    public void testStepTransaction() {
        
    }
    
}
