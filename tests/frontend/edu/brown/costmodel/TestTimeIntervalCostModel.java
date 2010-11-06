package edu.brown.costmodel;

import java.io.File;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

import org.apache.log4j.Level;
import org.voltdb.catalog.*;

import edu.brown.BaseTestCase;
import edu.brown.benchmark.tm1.procedures.DeleteCallForwarding;
import edu.brown.benchmark.tm1.procedures.GetAccessData;
import edu.brown.benchmark.tm1.procedures.GetSubscriberData;
import edu.brown.benchmark.tm1.procedures.UpdateLocation;
import edu.brown.catalog.CatalogUtil;
import edu.brown.designer.Designer;
import edu.brown.designer.DesignerHints;
import edu.brown.designer.DesignerInfo;
import edu.brown.designer.partitioners.BranchAndBoundPartitioner;
import edu.brown.designer.partitioners.PartitionPlan;
import edu.brown.rand.RandomDistribution;
import edu.brown.statistics.Histogram;
import edu.brown.utils.ProjectType;
import edu.brown.workload.AbstractTraceElement;
import edu.brown.workload.Workload;
import edu.brown.workload.TransactionTrace;
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
    private static Workload multi_workload;
    private static Workload single_workload;
    
    private TimeIntervalCostModel<SingleSitedCostModel> cost_model;
    private final Random rand = new Random();
    
    @Override
    protected void setUp() throws Exception {
        super.setUp(ProjectType.TM1);
        this.addPartitions(NUM_PARTITIONS);
        
        // Super hack! Walk back the directories and find out workload directory
        if (multi_workload == null) {
            File workload_file = this.getWorkloadFile(ProjectType.TM1); 
            
            // All Multi-Partition Txn Workload
            ProcedureNameFilter multi_filter = new ProcedureNameFilter();
            multi_filter.include(MULTIPARTITION_PROCEDURES);
            multi_filter.attach(new ProcedureLimitFilter(WORKLOAD_XACT_LIMIT));
            multi_workload = new Workload(catalog);
            ((Workload)multi_workload).load(workload_file.getAbsolutePath(), catalog_db, multi_filter);
            assert(multi_workload.getTransactionCount() > 0);

            // All Single-Partition Txn Workload
            ProcedureNameFilter single_filter = new ProcedureNameFilter();
            single_filter.include(SINGLEPARTITION_PROCEDURES);
            single_filter.attach(new ProcedureLimitFilter(WORKLOAD_XACT_LIMIT));
            single_workload = new Workload(catalog);
            ((Workload)single_workload).load(workload_file.getAbsolutePath(), catalog_db, single_filter);
            assert(single_workload.getTransactionCount() > 0);
        }
        this.cost_model = new TimeIntervalCostModel<SingleSitedCostModel>(catalog_db, SingleSitedCostModel.class, NUM_INTERVALS);
    }
    
    /**
     * testEstimateCost
     */
    public void testEstimateCost() throws Exception {
        // For now just check whether we get the same cost back for the same
        // workload... seems simple enough
        double cost0 = this.cost_model.estimateCost(catalog_db, multi_workload);
        double cost1 = this.cost_model.estimateCost(catalog_db, multi_workload);
        assertEquals(cost0, cost1);
        
        // Then make a new object and make sure that returns the same as well
        AbstractCostModel new_costmodel = new TimeIntervalCostModel<SingleSitedCostModel>(catalog_db, SingleSitedCostModel.class, NUM_INTERVALS);
        cost1 = new_costmodel.estimateCost(catalog_db, multi_workload);
        assertEquals(cost0, cost1);
    }
    
    /**
     * testIntervals
     */
    public void testIntervals() throws Exception {
        this.cost_model.estimateCost(catalog_db, multi_workload);
        for (int i = 0; i < NUM_INTERVALS; i++) {
            SingleSitedCostModel sub_model = this.cost_model.getCostModel(i);
            assertNotNull(sub_model);
            
            // Check Partition Access Histogram
            Histogram hist_access = sub_model.getQueryPartitionAccessHistogram();
            assertNotNull(hist_access);
            assertEquals(NUM_PARTITIONS, hist_access.getValueCount());
            
            // Check Java Execution Histogram
            Histogram hist_execute = sub_model.getJavaExecutionHistogram();
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
        final Map<Integer, Boolean> txn_for_partition = new HashMap<Integer, Boolean>();
        Workload.Filter filter = new Workload.Filter() {
            @Override
            protected void resetImpl() {
                // Nothing...
            }
            
            @Override
            protected FilterResult filter(AbstractTraceElement<? extends CatalogType> element) {
                if (element instanceof TransactionTrace) {
                    TransactionTrace xact = (TransactionTrace)element;
                    try {
                        Integer partition = p_estimator.getPartition(xact.getCatalogItem(catalog_db), xact.getParams());
                        if (partition == null) System.err.println(xact.debug(catalog_db));
                        assertNotNull(partition);
                        
                        if (txn_for_partition.containsKey(partition)) {
                            return (FilterResult.SKIP);    
                        }
                        txn_for_partition.put(partition, true);
                    } catch (Exception ex) {
                        ex.printStackTrace();
                        assert(false);
                    }
                }
                return (FilterResult.ALLOW);
            }
            @Override
            protected String debug() {
                return null;
            }
        };

        // Estimate the cost and examine the state of the estimation
        double cost = this.cost_model.estimateCost(catalog_db, single_workload, filter);
//        System.err.println(txn_for_partition);
        for (int i = 0; i < NUM_PARTITIONS; i++) {
            assert(txn_for_partition.containsKey(i)) : "No txn in workload for partition #" + i;
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
        Histogram h = new Histogram();
        double sigma = 3.5d;
        RandomDistribution.Zipf zipf = new RandomDistribution.Zipf(this.rand, 0, NUM_PARTITIONS, sigma);
        for (int i = 0; i < 100; i++) {
            h.put(zipf.nextInt());
        } // FOR
        final Map<Integer, Double> probs = new HashMap<Integer, Double>();
        for (int i = 0; i < NUM_PARTITIONS; i++) {
            Long cnt = h.get(i);
            if (cnt == null) cnt = 0l;
            probs.put(i, cnt / 100.0d);
        } // FOR
        
        Workload.Filter filter = new Workload.Filter() {
            @Override
            protected void resetImpl() {
                // Nothing...
            }
            
            @Override
            protected FilterResult filter(AbstractTraceElement<? extends CatalogType> element) {
                if (element instanceof TransactionTrace) {
                    TransactionTrace xact = (TransactionTrace)element;
                    try {
                        Integer partition = p_estimator.getPartition(xact.getCatalogItem(catalog_db), xact.getParams());
                        if (partition == null) System.err.println(xact.debug(catalog_db));
                        assertNotNull(partition);
                        
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
            protected String debug() {
                return null;
            }
        };

        // Estimate the cost and then check whether cost falls in our expected range
        // We expect that the entropy portion of the cost should be greater than 0.50
        double cost = this.cost_model.estimateCost(catalog_db, single_workload, filter);
//        System.err.println("Final Cost: " + cost);
//        System.err.println("Execution:  " + this.cost_model.last_execution_cost);
//        System.err.println("Entropy:    " + this.cost_model.last_entropy_cost);
        assert(cost >= 0.0d) : "Invalid cost: " + cost;
        assert(cost <= 2.0d) : "Invalid cost: " + cost;
        // FIXME assert(this.cost_model.last_entropy_cost > 0.50) : "Invalid Entropy: " + this.cost_model.last_entropy_cost;
    }
    
    /**
     * testConsistentCost
     */
    public void testConsistentCost() throws Exception {
        // We want to check that if we run the same workload multiple times that we get
        // the same cost each time
        int tries = 4;
        
        final DesignerHints hints = new DesignerHints();
        hints.limit_local_time = 1;
        hints.limit_total_time = 5;
        hints.enable_costmodel_caching = true;
        hints.enable_costmodel_java_execution = false;
        final PartitionPlan initial = PartitionPlan.createFromCatalog(catalog_db);
        
        // HACK: Enable debug output in BranchAndBoundPartitioner so that it slows the
        // the traversal. There is a race condition since we were able to speed things up
        BranchAndBoundPartitioner.LOG.setLevel(Level.DEBUG);
        
        Double last_cost = null;
        while (tries-- > 0) {
            final Database clone_db = CatalogUtil.cloneDatabase(catalog_db);
            // final TimeIntervalCostModel<SingleSitedCostModel> cm = new TimeIntervalCostModel<SingleSitedCostModel>(clone_db, SingleSitedCostModel.class, NUM_INTERVALS);
            AbstractCostModel cm = this.cost_model;

            double cost0 = cm.estimateCost(clone_db, single_workload);
            assert(cost0 > 0) : "[0] Invalid Cost: " + cost0;
            if (last_cost != null) {
                assertEquals("[0] Try #" + tries, cost0, last_cost.doubleValue());
            }
            
            DesignerInfo info = new DesignerInfo(clone_db, single_workload);
            info.setNumIntervals(NUM_INTERVALS);
            info.setPartitionerClass(BranchAndBoundPartitioner.class);
            info.setCostModel(cm);
            info.setCorrelationsFile(this.getCorrelationsFile(ProjectType.TM1).getAbsolutePath());
            Designer designer = new Designer(info, hints, info.getArgs());
            BranchAndBoundPartitioner local_search = (BranchAndBoundPartitioner)designer.getPartitioner();
            local_search.setUpperBounds(hints, initial, cost0, 12345);
            assertNotNull(local_search);
            
            // Now shovel through the Branch&Bound partitioner without actually doing anything
            // We should then get the exact same PartitionPlan back as we gave it
            PartitionPlan pplan = local_search.generate(hints);
            assertNotNull(pplan);
            assertEquals(initial, pplan);
            
            // Which then means we should get the exact same cost back
            initial.apply(clone_db);
            cm.clear(true);
            double cost1 = cm.estimateCost(clone_db, single_workload);
            assert(cost1 > 0) : "[1] Invalid Cost: " + cost0;
            assertEquals("[1] Try #" + tries, cost0, cost1);
            
            last_cost = cost0; 
        } // WHILE
        
    }
}
