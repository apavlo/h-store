package edu.brown.costmodel;

import java.io.File;
import java.lang.reflect.Field;
import java.util.BitSet;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

import org.voltdb.catalog.CatalogType;
import org.voltdb.catalog.Procedure;

import edu.brown.BaseTestCase;
import edu.brown.benchmark.tm1.procedures.DeleteCallForwarding;
import edu.brown.benchmark.tm1.procedures.GetAccessData;
import edu.brown.benchmark.tm1.procedures.GetSubscriberData;
import edu.brown.benchmark.tm1.procedures.UpdateLocation;
import edu.brown.hstore.HStoreConstants;
import edu.brown.rand.RandomDistribution;
import edu.brown.statistics.Histogram;
import edu.brown.statistics.ObjectHistogram;
import edu.brown.statistics.WorkloadStatistics;
import edu.brown.utils.CollectionUtil;
import edu.brown.utils.ProjectType;
import edu.brown.workload.AbstractTraceElement;
import edu.brown.workload.TransactionTrace;
import edu.brown.workload.Workload;
import edu.brown.workload.filters.Filter;
import edu.brown.workload.filters.ProcedureLimitFilter;
import edu.brown.workload.filters.ProcedureNameFilter;

public class TestTimeIntervalCostModel extends BaseTestCase {

    private static final long WORKLOAD_XACT_LIMIT = 1000;
    
    private static final String MULTIPARTITION_PROCEDURES[] = {
        DeleteCallForwarding.class.getSimpleName(),
        UpdateLocation.class.getSimpleName(),
    };
    
    private static final String SINGLEPARTITION_PROCEDURES[] = {
        GetAccessData.class.getSimpleName(),
        GetSubscriberData.class.getSimpleName()
    };
    
    private static final int NUM_PARTITIONS = 5;
    private static final int NUM_INTERVALS = 10;
    
    // Reading the workload takes a long time, so we only want to do it once
    private static Workload multip_workload;
    private static Workload singlep_workload;
    private static WorkloadStatistics stats;
    
    private TimeIntervalCostModel<SingleSitedCostModel> cost_model;
    private final Random rand = new Random();
    
    @Override
    protected void setUp() throws Exception {
        super.setUp(ProjectType.TM1);
        this.addPartitions(NUM_PARTITIONS);
        
        // Super hack! Walk back the directories and find out workload directory
        if (multip_workload == null) {
            File f = this.getWorkloadFile(ProjectType.TM1); 
            
            // All Multi-Partition Txn Workload
            ProcedureNameFilter multi_filter = new ProcedureNameFilter(false);
            multi_filter.include(MULTIPARTITION_PROCEDURES);
            multi_filter.attach(new ProcedureLimitFilter(WORKLOAD_XACT_LIMIT));
            multip_workload = new Workload(catalog);
            ((Workload)multip_workload).load(f, catalog_db, multi_filter);
            assert(multip_workload.getTransactionCount() > 0);

            // All Single-Partition Txn Workload
            ProcedureNameFilter single_filter = new ProcedureNameFilter(false);
            single_filter.include(SINGLEPARTITION_PROCEDURES);
            single_filter.attach(new ProcedureLimitFilter(WORKLOAD_XACT_LIMIT));
            singlep_workload = new Workload(catalog);
            ((Workload)singlep_workload).load(f, catalog_db, single_filter);
            assert(singlep_workload.getTransactionCount() > 0);
            
            // Workload Statistics
            f = this.getStatsFile(ProjectType.TM1);
            stats = new WorkloadStatistics(catalog_db);
            stats.load(f, catalog_db);
        }
        assertNotNull(multip_workload);
        assertNotNull(singlep_workload);
        assertNotNull(stats);
        
        this.cost_model = new TimeIntervalCostModel<SingleSitedCostModel>(catalogContext, SingleSitedCostModel.class, NUM_INTERVALS);
    }
    
    /**
     * testWeightedTxnEstimation
     */
    public void testWeightedTxnEstimation() throws Exception {
        int num_txns = 20;
        int num_intervals = 5;
        
        // Make a workload that has the same transaction in it multiple times
        Workload new_workload = new Workload(catalog);
        TransactionTrace multip_txn = CollectionUtil.first(multip_workload);
        Procedure catalog_proc = multip_txn.getCatalogItem(catalog_db);
        for (int i = 0; i < num_txns; i++) {
            TransactionTrace clone = (TransactionTrace)multip_txn.clone();
            clone.setTransactionId(i);
            clone.setTimestamps(new Long((i/5)*1000), new Long((i/5)*1000 + 100));
//            System.err.println(clone.debug(catalog_db) + "\n");
            new_workload.addTransaction(catalog_proc, clone);
        } // FOR
        assertEquals(num_txns, new_workload.getTransactionCount());
        TimeIntervalCostModel<SingleSitedCostModel> orig_costModel = new TimeIntervalCostModel<SingleSitedCostModel>(catalogContext, SingleSitedCostModel.class, num_intervals);
        double cost0 = orig_costModel.estimateWorkloadCost(catalogContext, new_workload);
        
        // Now change make a new workload that has the same multi-partition transaction
        // but this time it only has one but with a transaction weight
        // We should get back the exact same cost
//        System.err.println("+++++++++++++++++++++++++++++++++++++++++++++");
        new_workload = new Workload(catalog);
        for (int i = 0; i < num_txns/5; i++) {
            TransactionTrace clone = (TransactionTrace)multip_txn.clone();
            clone.setTransactionId(i);
            clone.setTimestamps(new Long((i*5)*1000), new Long((i*5)*1000 + 100));
            clone.setWeight(5);
//            System.err.println(clone.debug(catalog_db) + "\n");
            new_workload.addTransaction(catalog_proc, clone);
        } // FOR
        
        TimeIntervalCostModel<SingleSitedCostModel> new_costModel = new TimeIntervalCostModel<SingleSitedCostModel>(catalogContext, SingleSitedCostModel.class, num_intervals);
        double cost1 = new_costModel.estimateWorkloadCost(catalogContext, new_workload);
        assert(cost1 > 0);
        assertEquals(cost0, cost1, 0.001);
        
        // Now make sure the histograms match up
        Map<Field, Histogram<?>> orig_histograms = TestSingleSitedCostModel.getHistograms(orig_costModel);
        assertFalse(orig_histograms.isEmpty());
        Map<Field, Histogram<?>> new_histograms = TestSingleSitedCostModel.getHistograms(new_costModel);
        assertFalse(new_histograms.isEmpty());
        for (Field f : orig_histograms.keySet()) {
            Histogram<?> orig_h = orig_histograms.get(f);
            assertNotNull(orig_h);
            Histogram<?> new_h = new_histograms.get(f);
            assert(orig_h != new_h);
            assertNotNull(new_h);
            assertEquals(orig_h, new_h);
        } // FOR
    }
    
    /**
     * testEstimateCost
     */
    public void testEstimateCost() throws Exception {
        // For now just check whether we get the same cost back for the same
        // workload... seems simple enough
        double cost0 = cost_model.estimateWorkloadCost(catalogContext, multip_workload);
        double cost1 = cost_model.estimateWorkloadCost(catalogContext, multip_workload);
        assertEquals(cost0, cost1);
        
        // Then make a new object and make sure that returns the same as well
        AbstractCostModel new_costmodel = new TimeIntervalCostModel<SingleSitedCostModel>(catalogContext, SingleSitedCostModel.class, NUM_INTERVALS);
        cost1 = new_costmodel.estimateWorkloadCost(catalogContext, multip_workload);
        assertEquals(cost0, cost1);
    }
    
    /**
     * testIntervals
     */
    public void testIntervals() throws Exception {
        this.cost_model.estimateWorkloadCost(catalogContext, multip_workload);
        for (int i = 0; i < NUM_INTERVALS; i++) {
            SingleSitedCostModel sub_model = this.cost_model.getCostModel(i);
            assertNotNull(sub_model);
            
            // Check Partition Access Histogram
            Histogram<Integer> hist_access = sub_model.getQueryPartitionAccessHistogram();
            assertNotNull(hist_access);
            assertEquals(NUM_PARTITIONS, hist_access.getValueCount());
            
            // Check Java Execution Histogram
            Histogram<Integer> hist_execute = sub_model.getJavaExecutionHistogram();
            assertNotNull(hist_execute);
//            System.err.println("Interval #" + i + "\n" + hist_execute);
//            assertEquals(1, hist_execute.getValueCount());
        } // FOR
    }
    
    /**
     * testSinglePartitionedUniformWorkload
     */
    public void testSinglePartitionedUniformWorkload() throws Exception {
        // This workload should will only consist of single-partition txns and 
        // is evenly spread out across all partitions
        final BitSet txn_for_partition = new BitSet(NUM_PARTITIONS);
        Filter filter = new Filter() {
            @Override
            protected void resetImpl() {
                // Nothing...
            }
            
            @Override
            protected FilterResult filter(AbstractTraceElement<? extends CatalogType> element) {
                if (element instanceof TransactionTrace) {
                    TransactionTrace xact = (TransactionTrace)element;
                    try {
                        int partition = p_estimator.getBasePartition(xact);
                        if (partition == HStoreConstants.NULL_PARTITION_ID) System.err.println(xact.debug(catalog_db));
                        assert(partition != HStoreConstants.NULL_PARTITION_ID);
                        
                        if (txn_for_partition.get(partition)) {
                            return (FilterResult.SKIP);    
                        }
                        txn_for_partition.set(partition);
                    } catch (Exception ex) {
                        ex.printStackTrace();
                        assert(false);
                    }
                }
                return (FilterResult.ALLOW);
            }
            @Override
            public String debugImpl() {
                return null;
            }
        };

        // Estimate the cost and examine the state of the estimation
        double cost = this.cost_model.estimateWorkloadCost(catalogContext, singlep_workload, filter, null);
//        System.err.println(txn_for_partition);
        for (int i = 0; i < NUM_PARTITIONS; i++) {
            assert(txn_for_partition.get(i)) : "No txn in workload for partition #" + i;
        } // FOR
        assert(cost >= 0.0d) : "Invalid cost: " + cost;
        assert(cost <= 2.0d) : "Invalid cost: " + cost;
//        System.err.println("Final Cost: " + cost);
//        System.err.println("Execution:  " + this.cost_model.last_execution_cost);
//        System.err.println("Entropy:    " + this.cost_model.last_entropy_cost);
    }
    
    /**
     * testSinglePartitionSkewedWorkload
     */
    public void testSinglePartitionSkewedWorkload() throws Exception {
        // First construct a zipfian-based histogram of partitions and then create a filter that
        // will selectively prune out txns based on the frequencies in the histogram
        Histogram<Integer> h = new ObjectHistogram<Integer>();
        double sigma = 3.5d;
        RandomDistribution.Zipf zipf = new RandomDistribution.Zipf(this.rand, 0, NUM_PARTITIONS, sigma);
        for (int i = 0; i < 100; i++) {
            h.put(zipf.nextInt());
        } // FOR
        final Map<Integer, Double> probs = new HashMap<Integer, Double>();
        for (int i = 0; i < NUM_PARTITIONS; i++) {
            long cnt = h.get(i, 0);
            probs.put(i, cnt / 100.0d);
        } // FOR
        
        Filter filter = new Filter() {
            @Override
            protected void resetImpl() {
                // Nothing...
            }
            
            @Override
            protected FilterResult filter(AbstractTraceElement<? extends CatalogType> element) {
                if (element instanceof TransactionTrace) {
                    TransactionTrace xact = (TransactionTrace)element;
                    try {
                        int partition = p_estimator.getBasePartition(xact);
                        if (partition == HStoreConstants.NULL_PARTITION_ID) System.err.println(xact.debug(catalog_db));
                        assert(partition != HStoreConstants.NULL_PARTITION_ID);
                        
                        double next = rand.nextDouble();
                        double prob = probs.get(partition);
                        boolean skip = (next > prob);
                        // System.err.println("Partition=" + partition + ", Prob=" + prob + ", Next=" + next + ", Skip=" + skip);
                        if (skip) return (FilterResult.SKIP);
                    } catch (Exception ex) {
                        ex.printStackTrace();
                        assert(false);
                    }
                }
                return (FilterResult.ALLOW);
            }
            @Override
            public String debugImpl() {
                return null;
            }
        };

        // Estimate the cost and then check whether cost falls in our expected range
        // We expect that the entropy portion of the cost should be greater than 0.50
        double cost = this.cost_model.estimateWorkloadCost(catalogContext, singlep_workload, filter, null);
//        System.err.println("Final Cost: " + cost);
//        System.err.println("Execution:  " + this.cost_model.last_execution_cost);
//        System.err.println("Entropy:    " + this.cost_model.last_entropy_cost);
        assert(cost >= 0.0d) : "Invalid cost: " + cost;
        assert(cost <= 2.0d) : "Invalid cost: " + cost;
        // FIXME assert(this.cost_model.last_entropy_cost > 0.50) : "Invalid Entropy: " + this.cost_model.last_entropy_cost;
    }
    
// FIXME 2012-17-10
// This is broken after changing PartitionSet internals
//    /**
//     * testConsistentCost
//     */
//    public void testConsistentCost() throws Exception {
//        // We want to check that if we run the same workload multiple times that we get
//        // the same cost each time
//        int tries = 4;
//        
//        final DesignerHints hints = new DesignerHints();
//        hints.limit_local_time = 1;
//        hints.limit_total_time = 5;
//        hints.enable_costmodel_caching = false;
//        hints.enable_costmodel_java_execution = false;
//        hints.max_memory_per_partition = Long.MAX_VALUE;
//        hints.enable_vertical_partitioning = false;
//        final PartitionPlan initial = PartitionPlan.createFromCatalog(catalog_db);
//        
//        // HACK: Enable debug output in BranchAndBoundPartitioner so that it slows the
//        // the traversal. There is a race condition since we were able to speed things up
//        BranchAndBoundPartitioner.LOG.setLevel(Level.DEBUG);
//        
//        System.err.println("INITIAL PARTITIONPLAN:\n" + initial);
//        
//        Double last_cost = null;
//        while (tries-- > 0) {
//            final Database clone_db = CatalogCloner.cloneDatabase(catalog_db);
//            CatalogContext clone_catalogContext = new CatalogContext(clone_db.getCatalog());
//            // final TimeIntervalCostModel<SingleSitedCostModel> cm = new TimeIntervalCostModel<SingleSitedCostModel>(clone_db, SingleSitedCostModel.class, NUM_INTERVALS);
////            AbstractCostModel cm = new SingleSitedCostModel(catalog_db, p_estimator);
//            AbstractCostModel cm = this.cost_model;
//
//            double cost0 = cm.estimateWorkloadCost(clone_catalogContext, singlep_workload);
//            assert(cost0 > 0) : "[0] Invalid Cost: " + cost0;
//            if (last_cost != null) {
//                assertEquals("[0] Try #" + tries, cost0, last_cost.doubleValue());
//            }
//            
//            DesignerInfo info = new DesignerInfo(clone_catalogContext, singlep_workload);
//            info.setStats(stats);
//            info.setNumIntervals(NUM_INTERVALS);
//            info.setPartitionerClass(BranchAndBoundPartitioner.class);
//            info.setCostModel(cm);
//            info.setMappingsFile(this.getParameterMappingsFile(ProjectType.TM1));
//            
//            Designer designer = new Designer(info, hints, info.getArgs());
//            BranchAndBoundPartitioner local_search = (BranchAndBoundPartitioner)designer.getPartitioner();
//            local_search.setUpperBounds(hints, initial, cost0, 12345);
//            assertNotNull(local_search);
//            
//            // Now shovel through the Branch&Bound partitioner without actually doing anything
//            // We should then get the exact same PartitionPlan back as we gave it
//            PartitionPlan pplan = null;
//            try {
//                pplan = local_search.generate(hints);
//            } catch (Exception ex) {
//                System.err.println("GRAPH: " + FileUtil.writeStringToTempFile(GraphvizExport.export(local_search.getAcessGraph(), "tm1"), "dot"));
//                throw ex;
//            }
//            assertNotNull(pplan);
//            assertEquals(initial, pplan);
//            
//            // Which then means we should get the exact same cost back
//            initial.apply(clone_db);
//            cm.clear(true);
//            double cost1 = cm.estimateWorkloadCost(catalogContext, singlep_workload);
//            assert(cost1 > 0) : "[1] Invalid Cost: " + cost0;
//            assertEquals("[1] Try #" + tries, cost0, cost1);
//            
//            last_cost = cost0; 
//        } // WHILE
//        
//    }
}
