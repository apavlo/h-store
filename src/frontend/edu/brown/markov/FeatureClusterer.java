package edu.brown.markov;

import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.collections15.map.ListOrderedMap;
import org.apache.commons.collections15.set.ListOrderedSet;
import org.apache.commons.pool.BasePoolableObjectFactory;
import org.apache.log4j.Logger;
import org.voltdb.CatalogContext;
import org.voltdb.catalog.Procedure;

import weka.classifiers.Classifier;
import weka.classifiers.meta.FilteredClassifier;
import weka.classifiers.trees.J48;
import weka.clusterers.AbstractClusterer;
import weka.clusterers.EM;
import weka.clusterers.FilteredClusterer;
import weka.core.Attribute;
import weka.core.Instance;
import weka.core.Instances;
import weka.filters.Filter;
import weka.filters.unsupervised.attribute.NumericToNominal;
import edu.brown.catalog.CatalogUtil;
import edu.brown.costmodel.MarkovCostModel;
import edu.brown.hstore.estimators.markov.MarkovEstimator;
import edu.brown.logging.LoggerUtil;
import edu.brown.logging.LoggerUtil.LoggerBoolean;
import edu.brown.markov.containers.MarkovGraphsContainerUtil;
import edu.brown.markov.containers.MarkovGraphsContainer;
import edu.brown.markov.features.BasePartitionFeature;
import edu.brown.markov.features.FeatureUtil;
import edu.brown.pools.FastObjectPool;
import edu.brown.pools.Poolable;
import edu.brown.statistics.ObjectHistogram;
import edu.brown.utils.ArgumentsParser;
import edu.brown.utils.CollectionUtil;
import edu.brown.utils.FileUtil;
import edu.brown.utils.PartitionEstimator;
import edu.brown.utils.PartitionSet;
import edu.brown.utils.StringUtil;
import edu.brown.utils.UniqueCombinationIterator;
import edu.brown.workload.TransactionTrace;
import edu.brown.workload.Workload;

/**
 * 
 * @author pavlo
 */
public class FeatureClusterer {
    private static final Logger LOG = Logger.getLogger(FeatureClusterer.class);
    private static final LoggerBoolean debug = new LoggerBoolean();
    private static final LoggerBoolean trace = new LoggerBoolean();
    static {
        LoggerUtil.attachObserver(LOG, debug, trace);
    }

    public enum SplitType {
        /** Training Workload Percentage */
        TRAINING        (0.40),
        
        /** Validation Workload Percentage */
        VALIDATION      (0.60),
        
        /** Testing Workload Percentage */        
        TESTING         (0.00);
        
        private final double percentage;
        
        private SplitType(double percentage) {
            this.percentage = percentage;
        }
        public double getPercentage() {
            return this.percentage;
        }
    }
    
    // ----------------------------------------------------------------------------
    // DEFAULT CONFIGURATION VALUES
    // ----------------------------------------------------------------------------
    
    /** For each search round, we will only propagate the attributes found this these top-k AttibuteSets */ 
    private static final double DEFAULT_ATTRIBUTESET_TOP_K = 0.10;
    
    /** Number of threads to use per thread pool */
    private static final int DEFAULT_NUM_THREADS = 2;
    
    /** Number of search rounds in findBestMarkovAttributeSet */
    private static final int DEFAULT_NUM_ROUNDS = 10;

    // ----------------------------------------------------------------------------
    // MARKOVGRAPHSCONTAINER WRAPPER
    // ----------------------------------------------------------------------------
    
    private static class TxnToClusterMarkovGraphsContainer extends MarkovGraphsContainer {
        
        /**
         * Hackish cross-reference table to go from the TransactionId to Cluster#
         */
        private final Map<Long, Integer> txnid_cluster_xref = new HashMap<Long, Integer>();
        
        @Override
        public MarkovGraph getFromParams(Long txn_id, int base_partition, Object[] params, Procedure catalog_proc) {
            // Look-up what cluster our TransactionTrace belongs to
            Integer cluster = this.txnid_cluster_xref.get(txn_id);
            assert(cluster != null) : "Failed to initialize TransactionId->Cluster# xref for txn #" + txn_id;
            return this.get(cluster, catalog_proc);
        }
        
        /**
         * Map a TransactionId to a ClusterId
         * @param txn_id
         * @param cluster_id
         */
        public void addTransactionClusterXref(long txn_id, int cluster_id) {
            this.txnid_cluster_xref.put(txn_id, cluster_id);
        }
        
        @Override
        public void clear() {
            super.clear();
            this.txnid_cluster_xref.clear();
        }
    }
    
    // ----------------------------------------------------------------------------
    // EXECUTION STATE
    // ----------------------------------------------------------------------------

    /**
     * ExecutionState Factory
     */
    private static class ExecutionStateFactory extends BasePoolableObjectFactory {
        private final FeatureClusterer fclusterer;
        
        public ExecutionStateFactory(FeatureClusterer fclusterer) {
            this.fclusterer = fclusterer;
        }
        @Override
        public Object makeObject() throws Exception {
            return this.fclusterer.new ExecutionState();
        }
        @Override
        public void passivateObject(Object obj) throws Exception {
            ExecutionState state = (ExecutionState)obj;
            state.finish();
        }
    } // END CLASS
    
    /**
     * 
     */
    private class ExecutionState implements Poolable {
        /**
         * Current Clusterer for this ExecutionState
         */
        AbstractClusterer clusterer;
        /**
         * Set of all the ClusterIds that we have seen
         */
        final Set<Integer> cluster_ids = new HashSet<Integer>();
        /**
         * We want to always split the MarkovGraphContainers by base partition, since we already know
         * that this is going to be the best predictor
         */
        final TxnToClusterMarkovGraphsContainer markovs_per_partition[];
        /**
         * Then we have a costmodel for each PartitionId 
         */
        final MarkovCostModel costmodels_per_partition[];
        /**
         * And a TransactionEstimator for each PartitionId
         */
        final MarkovEstimator t_estimators_per_partition[];
        /**
         * Histogram of Clusters Per Partition
         */
        final ObjectHistogram<Integer> clusters_per_partition[];
        
        int c_counters[] = new int[] {
            0,      // Single-P
            0,      // Multi-P
            0,      // Known Clusters
        };
        int t_counters[] = new int[] {
            0,      // Single-P
            0,      // Multi-P
            0,      // Total # of Txns
        };
        
        /**
         * Constructor
         */
        @SuppressWarnings("unchecked")
        private ExecutionState() {
            // We allocate a complete array for all of the partitions in the catalog
            this.markovs_per_partition = new TxnToClusterMarkovGraphsContainer[FeatureClusterer.this.total_num_partitions];
            this.costmodels_per_partition = new MarkovCostModel[FeatureClusterer.this.total_num_partitions];
            this.t_estimators_per_partition = new MarkovEstimator[FeatureClusterer.this.total_num_partitions];
            this.clusters_per_partition = (ObjectHistogram<Integer>[])new ObjectHistogram<?>[FeatureClusterer.this.total_num_partitions];
            
            // But then only initialize the partition-specific data structures
            for (int p : FeatureClusterer.this.all_partitions) {
                this.clusters_per_partition[p] = new ObjectHistogram<Integer>();
                this.markovs_per_partition[p] = new TxnToClusterMarkovGraphsContainer();
                this.t_estimators_per_partition[p] = new MarkovEstimator(catalogContext, p_estimator, this.markovs_per_partition[p]);
                this.costmodels_per_partition[p] = new MarkovCostModel(catalogContext, p_estimator, this.t_estimators_per_partition[p], thresholds);
            } // FOR
        }
        
        public void init(AbstractClusterer clusterer) {
            this.clusterer = clusterer;
        }
        
        @Override
        public boolean isInitialized() {
            return (this.clusterer != null);
        }
        
        public void finish() {
            this.clusterer = null;
            this.cluster_ids.clear();
            for (int p : FeatureClusterer.this.all_partitions) {
                this.clusters_per_partition[p].clear();
                this.markovs_per_partition[p].clear();

                // It's lame, but we need to put this here so that...
                this.costmodels_per_partition[p] = new MarkovCostModel(catalogContext, p_estimator, this.t_estimators_per_partition[p], thresholds);
            } // FOR
            
            // Reset Counters
            for (int i = 0; i < this.c_counters.length; i++) {
                this.c_counters[i] = 0;
                this.t_counters[i] = 0;
            } // FOR
        }
    }
    
    
    // ----------------------------------------------------------------------------
    // DATA MEMBERS
    // ----------------------------------------------------------------------------

    /**
     * We also maintain a "global" MarkovGraphContainer that consumes all transactions
     * We will use to compare whether our cluster-specific models do better than the global one
     * This is automatically connected to the FeatureClusterer's base partition cache, so we
     * don't have to do anything special to get out what we need here
     */
    private final MarkovGraphsContainer global_markov = new MarkovGraphsContainer() {
        public MarkovGraph getFromParams(long txn_id, int base_partition, Object[] params, Procedure catalog_proc) {
            return (this.get(base_partition, catalog_proc));
        };
    };
    
    /**
     * Global Cost Model
     */
    private final MarkovCostModel global_costmodel;
    /**
     * Global TransactionEstimator
     */
    private final MarkovEstimator global_t_estimator;
    
    /**
     * Global Counters
     */
    private double total_g_cost = 0.0d;
    private int g_counters[] = new int[] {
        0,      // Single-P
        0,      // Multi-P
        0,      // Known Clusters
    };
    
    private final Instances splits[] = new Instances[SplitType.values().length];
    private final double split_percentages[] = new double[SplitType.values().length];
    private final int split_counts[] = new int[SplitType.values().length];

    private final CatalogContext catalogContext;
    private final Procedure catalog_proc;
    private final Workload workload;
    private final EstimationThresholds thresholds;
    private final PartitionEstimator p_estimator;
    private final Random rand = new Random(); // FIXME
    private final PartitionSet all_partitions;
    private final int total_num_partitions;
    private final FastObjectPool<ExecutionState> state_pool = new FastObjectPool<ExecutionState>(new ExecutionStateFactory(this));

    /** We want to have just one thread pool for calculate threads */
    private final ExecutorService calculate_threadPool;
    
    private double round_topk = DEFAULT_ATTRIBUTESET_TOP_K;
    private int num_rounds = DEFAULT_NUM_ROUNDS;
    
    private final Map<Long, PartitionSet> cache_all_partitions = new HashMap<Long, PartitionSet>();
    private final Map<Long, Integer> cache_base_partition = new HashMap<Long, Integer>();


    /**
     * Constructor
     * @param catalogContext
     * @param catalog_proc
     * @param workload
     * @param correlations
     * @param all_partitions
     * @param num_threads
     */
    public FeatureClusterer(CatalogContext catalogContext, Procedure catalog_proc, Workload workload, PartitionSet all_partitions, int num_threads) {
        this.catalogContext = catalogContext;
        this.catalog_proc = catalog_proc;
        this.workload = workload;
        this.thresholds = new EstimationThresholds(); // FIXME
        this.p_estimator = new PartitionEstimator(catalogContext);
        this.all_partitions = all_partitions;
        this.total_num_partitions = catalogContext.numberOfPartitions;
        this.calculate_threadPool = Executors.newFixedThreadPool(num_threads);
        
        for (SplitType type : SplitType.values()) {
            this.split_percentages[type.ordinal()] = type.percentage;
        } // FOR
        
        this.global_t_estimator = new MarkovEstimator(this.catalogContext, this.p_estimator, this.global_markov);
        this.global_costmodel = new MarkovCostModel(catalogContext, this.p_estimator, this.global_t_estimator, this.thresholds);
        for (Integer p : FeatureClusterer.this.all_partitions) {
            this.global_markov.getOrCreate(p, FeatureClusterer.this.catalog_proc).initialize();
        } // FOR
    }

    /**
     * Constructor
     */
    public FeatureClusterer(CatalogContext catalogContext, Procedure catalog_proc, Workload workload, PartitionSet all_partitions) {
        this(catalogContext, catalog_proc, workload, all_partitions, DEFAULT_NUM_THREADS);
    }
    
    protected final void cleanup() {
//        this.generate_threadPool.shutdownNow();
        this.calculate_threadPool.shutdownNow();
    }
    
    public void setNumRounds(int numRounds) {
        this.num_rounds = numRounds;
        if (debug.val) LOG.debug("Number of Rounds: " + numRounds);
    }
    public void setSplitPercentage(SplitType type, double percentage) {
        this.split_percentages[type.ordinal()] = percentage;
        if (debug.val) LOG.debug(String.format("%s Split Percentage: ", type.name(), percentage));
    }
    public void setAttributeTopK(double topk) {
        this.round_topk = topk;
        if (debug.val) LOG.debug("Attribute Top-K: " + topk);
    }
    
    protected MarkovCostModel getGlobalCostModel() {
        return this.global_costmodel;
    }
    protected MarkovGraphsContainer getGlobalMarkovGraphs() {
        return this.global_markov;
    }
    protected int[] getGlobalCounters() {
        return (this.g_counters);
    }
    
    /**
     * 
     * @param data
     * @return
     */
    protected Instances[] splitWorkload(Instances data) {
        int offset = 0;
        int all_cnt = data.numInstances();
        for (SplitType stype : SplitType.values()) {
            int idx = stype.ordinal();
            this.split_counts[idx] = (int)Math.round(all_cnt * stype.percentage);
            
            try {
                this.splits[idx] = new Instances(data, offset, this.split_counts[idx]);
            
                // Apply NumericToNominal filter!
                NumericToNominal filter = new NumericToNominal();
                filter.setInputFormat(this.splits[idx]);
                this.splits[idx] = Filter.useFilter(this.splits[idx], filter);
                
            } catch (Exception ex) {
                throw new RuntimeException("Failed to split " + stype + " workload", ex);
            }
            
            offset += this.split_counts[idx];
            if (debug.val) LOG.debug(String.format("%-12s%d", stype.toString()+":", this.split_counts[idx]));
        } // FOR
        return (this.splits);
    }

    // ----------------------------------------------------------------------------
    // CACHING METHODS
    // ----------------------------------------------------------------------------
    
    private int getBasePartition(TransactionTrace txn_trace) {
        Long txn_id = Long.valueOf(txn_trace.getTransactionId());
        Integer base_partition = this.cache_base_partition.get(txn_id);
        if (base_partition == null) {
            try {
                base_partition = this.p_estimator.getBasePartition(txn_trace);
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
            this.cache_base_partition.put(txn_id, base_partition);
        }
        return (base_partition.intValue());
    }
    
    private PartitionSet getAllPartitions(TransactionTrace txn_trace) {
        Long txn_id = Long.valueOf(txn_trace.getTransactionId());
        PartitionSet all_partitions = this.cache_all_partitions.get(txn_id);
        if (all_partitions == null) {
            all_partitions = new PartitionSet();
            try {
                this.p_estimator.getAllPartitions(all_partitions, txn_trace);
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
            this.cache_all_partitions.put(txn_id, all_partitions);
        }
        return (all_partitions);
    }
    
    // ----------------------------------------------------------------------------
    // CALCULATION METHODS
    // ----------------------------------------------------------------------------
    
    /**
     * 
     * @param fset
     * @param data
     * @param catalog_proc
     * @throws Exception
     */
    @SuppressWarnings("unchecked")
    protected MarkovAttributeSet calculate(final Instances data) throws Exception {

        // ----------------------------------------------------------------------------
        // Split the input data set into separate data sets
        // ----------------------------------------------------------------------------
        if (debug.val) LOG.debug(String.format("Splitting %d instances", data.numInstances()));
        this.splitWorkload(data);

        // ----------------------------------------------------------------------------
        // Calculate global information
        // ----------------------------------------------------------------------------
        if (debug.val) LOG.debug("Calculating Global MarkovGraph cost");
        this.calculateGlobalCost();
        
        // ----------------------------------------------------------------------------
        // Perform Feed-Forward Selection
        // ----------------------------------------------------------------------------
        Attribute base_partition_attr = data.attribute(FeatureUtil.getFeatureKeyPrefix(BasePartitionFeature.class));
        assert(base_partition_attr != null);
        Integer base_partition_idx = base_partition_attr.index();
        assert(base_partition_idx != null);

        // Get the list of all the attributes that we are going to want to try to cluster on
        // We want to always remove the first attribute because that's the TransactionId
        List<Attribute> temp = (List<Attribute>)CollectionUtil.addAll(new ArrayList<Attribute>(), data.enumerateAttributes());
        
        // Remove the TransactionId and BasePartition features
        temp.remove(FeatureExtractor.TXNID_ATTRIBUTE_IDX);
        temp.remove(base_partition_idx);

        Collections.shuffle(temp, this.rand);
        ListOrderedSet<Attribute> all_attributes = new ListOrderedSet<Attribute>();
        all_attributes.addAll(temp);
        
        // List of all AttributeSets ever created
        final SortedSet<MarkovAttributeSet> all_asets = new TreeSet<MarkovAttributeSet>();

        // The AttributeSets created in each round
        final SortedSet<MarkovAttributeSet> round_asets = new TreeSet<MarkovAttributeSet>();
        final Map<MarkovAttributeSet, AbstractClusterer> round_clusterers = new HashMap<MarkovAttributeSet, AbstractClusterer>();

        // The best AttributeSet + Clusterer we've seen thus far
        MarkovAttributeSet best_aset = null;
        AbstractClusterer best_clusterer = null;
        boolean found_new_best = true;
        
        int round = 0;
        while (round++ < this.num_rounds && found_new_best) {
            round_asets.clear();
            round_clusterers.clear();
            
            if (debug.val) {
                Map<String, Object> m0 = new ListOrderedMap<String, Object>();
                m0.put("Round #", String.format("%02d", round));
                m0.put("Number of Partitions", this.all_partitions.size());
                m0.put("Number of Attributes", all_attributes.size());
                m0.put("Best Set", best_aset);
                m0.put("Best Cost", (best_aset != null ? best_aset.getCost() : null));
                
                Map<String, Object> m1 = new ListOrderedMap<String, Object>();
                for (SplitType stype : SplitType.values()) {
                    String key = String.format("# of %s Instances", stype.name());
                    String val = String.format("%-8s [%.02f]", this.split_counts[stype.ordinal()], stype.percentage);
                    m1.put(key, val);    
                } // FOR
                
                LOG.debug("\n" + StringUtil.formatMaps(":", true, true, false, false, true, true, m0, m1));
            }

            final Iterable<Set<Attribute>> it = UniqueCombinationIterator.factory(all_attributes, round);
            final List<Set<Attribute>> sets = (List<Set<Attribute>>)CollectionUtil.addAll(new ArrayList<Set<Attribute>>(), it);

            final int num_sets = sets.size();
            final CountDownLatch latch = new CountDownLatch(num_sets);
            final AtomicInteger aset_ctr = new AtomicInteger(0);
            
            for (final Set<Attribute> s : sets) {
                Runnable r = new Runnable() {
                    @Override
                    public void run() {
                        MarkovAttributeSet aset = new MarkovAttributeSet(s);
                        AbstractClusterer clusterer = null;
//                        if (aset_ctr.get() <= 0) {
                            if (trace.val) LOG.trace("Constructing AttributeSet: " + aset);
                            try {
                                clusterer = FeatureClusterer.this.calculateAttributeSetCost(aset);
                            } catch (Exception ex) {
                                LOG.fatal("Failed to calculate MarkovAttributeSet cost for " + aset, ex);
                                throw new RuntimeException(ex);
                            }
                            assert(aset != null);
                            assert(clusterer != null);
                            round_asets.add(aset);
                            round_clusterers.put(aset, clusterer);
                            all_asets.add(aset);
                            if (debug.val) {
                                int my_ctr = aset_ctr.getAndIncrement();
                                LOG.debug(String.format("[%03d] %s => %.03f", my_ctr, aset, aset.getCost()));
                            }
//                        }
                        latch.countDown();
                        
                    }
                };
                this.calculate_threadPool.execute(r);
            } // FOR
            
            // Wait until they all finish
            if (debug.val) LOG.debug(String.format("Waiting for %d calculateAttributeSetCosts threads to finish", num_sets));
            latch.await();
            
            // Now figure out what the top-k MarkovAttributeSets from this round
            // For now we'll explode out all of the attributes that they contain and throw that into a set
            // of candidate attributes for the next round
            all_attributes.clear();
            int top_k = (int)Math.round(round_asets.size() * this.round_topk);
            for (MarkovAttributeSet aset : round_asets) {
                all_attributes.addAll(aset);
                if (debug.val) LOG.debug(String.format("%.03f\t%s", aset.getCost(), aset.toString()));
                if (top_k-- == 0) break;
            } // FOR
            // if (round == 1) all_attributes.add(data.attribute(1));
            
            MarkovAttributeSet round_best = round_asets.first();
            assert(round_best != null);
            if (best_aset == null || round_best.getCost() < best_aset.getCost()) {
                best_aset = round_best;
                best_clusterer = round_clusterers.get(round_best);
            } else {
                found_new_best = false;
            }
            
            if (debug.val) LOG.debug(String.format("Next Round Attributes [size=%d]: %s", all_attributes.size(), MarkovAttributeSet.toString(all_attributes)));
        } // WHILE (round)
        
        this.generateDecisionTree(best_clusterer, best_aset, data);
        
        return (best_aset);
    }

    /**
     * Calculate the cost of a global MarkovGraph estimator 
     * @throws Exception
     */
    protected void calculateGlobalCost() throws Exception {
        final Instances trainingData = this.splits[SplitType.TRAINING.ordinal()];
        assert(trainingData != null);
        final Instances validationData = this.splits[SplitType.VALIDATION.ordinal()];
        assert(validationData != null);
        
        // ----------------------------------------------------------------------------
        // BUILD GLOBAL MARKOVGRAPH
        // ----------------------------------------------------------------------------
        for (int i = 0, cnt = trainingData.numInstances(); i < cnt; i++) {
            // Grab the Instance and throw it at the the clusterer to get the target cluster
            // The original data set is going to have the txn id that we need to grab 
            // the proper TransactionTrace record from the workload
            Instance inst = trainingData.instance(i);
            long txn_id = FeatureUtil.getTransactionId(inst);
            TransactionTrace txn_trace = this.workload.getTransaction(txn_id);
            assert(txn_trace != null) : "Invalid TxnId #" + txn_id + "\n" + inst;

            // Figure out which base partition this txn would execute on
            // because we want divide the MarkovGraphContainers by the base partition
            int base_partition = this.getBasePartition(txn_trace);
            
            // Update Global MarkovGraph
            MarkovGraph markov = this.global_markov.get(base_partition, this.catalog_proc);
            assert(markov != null) : "Failed to get Global MarkovGraph for partition #" + base_partition;
            markov.processTransaction(txn_trace, this.p_estimator);
        } // FOR

        // ----------------------------------------------------------------------------
        // BUILD GLOBAL COST MODELS
        // ----------------------------------------------------------------------------
        for (Integer partition : FeatureClusterer.this.all_partitions) {
            MarkovGraph m = this.global_markov.get(partition, this.catalog_proc);
            assert(m != null);
            m.calculateProbabilities(catalogContext.getAllPartitionIds());
            assert(m.isValid()) : "The MarkovGraph at Partition #" + partition + " is not valid!";
        } // FOR
        if (debug.val) LOG.debug(String.format("Finished initializing GLOBAL MarkovCostModel"));

        // ----------------------------------------------------------------------------
        // ESTIMATE GLOBAL COST
        // ----------------------------------------------------------------------------
        int validationCnt = validationData.numInstances();
        int recalculate_ctr = 0;
        for (int i = 0; i < validationCnt; i++) {
            if (trace.val && i > 0 && i % 1000 == 0) LOG.trace(String.format("TransactionTrace %d/%d", i, validationCnt));
            Instance inst = validationData.instance(i);
            long txn_id = FeatureUtil.getTransactionId(inst);
            TransactionTrace txn_trace = this.workload.getTransaction(txn_id);
            assert(txn_trace != null);
            int base_partition = this.getBasePartition(txn_trace);

            // Skip any txn that executes on a partition that we're not evaluating
            if (this.all_partitions.contains(base_partition) == false) continue;

            // Ok so now let's figure out what this mofo is going to do...
            PartitionSet partitions = this.getAllPartitions(txn_trace);
            boolean singlepartitioned = (partitions.size() == 1);
            
            // Estimate Global MarkovGraph Cost
            double g_cost = this.global_costmodel.estimateTransactionCost(catalogContext, txn_trace);
            if (g_cost > 0) {
                this.total_g_cost += g_cost;
                this.g_counters[singlepartitioned ? 0 : 1]++;
             
                MarkovGraph m = this.global_markov.get(base_partition, this.catalog_proc);
                assert(m != null);
                m.processTransaction(txn_trace, p_estimator);
                // m.calculateProbabilities();
                recalculate_ctr++;
            }
        } // FOR
        if (debug.val) LOG.debug(String.format("Recalculated global probabilities %d out of %d times", recalculate_ctr, validationCnt));
    }
    
    protected Map<Integer, MarkovGraphsContainer> constructMarkovModels(MarkovAttributeSet aset, Instances data) throws Exception {
        
        // Create an ExecutionState for this run
        ExecutionState state = (ExecutionState)this.state_pool.borrowObject();
        state.init(this.createClusterer(aset, data));
        
        // Construct the MarkovGraphs for each Partition/Cluster using the Training Data Set
        this.generateMarkovGraphs(state, data);
        
        // Generate the MarkovModels for the different partitions+clusters
        this.generateMarkovCostModels(state);
        
        Map<Integer, MarkovGraphsContainer> ret = new HashMap<Integer, MarkovGraphsContainer>();
        for (int p = 0; p < state.markovs_per_partition.length; p++) {
            ret.put(p, state.markovs_per_partition[p]);
        } // FOR
        return (ret);
    }
    
    /**
     * 
     * @param catalog_proc
     * @param attributes
     * @param trainingData
     * @param validationData
     * @return
     * @throws Exception
     */
    public AbstractClusterer calculateAttributeSetCost(final MarkovAttributeSet aset) throws Exception {
        // Build our clusterer
        if (debug.val) LOG.debug("Training Clusterer - " + aset);
        AbstractClusterer clusterer = this.createClusterer(aset, this.splits[SplitType.TRAINING.ordinal()]);
        
        // Create an ExecutionState for this run
        ExecutionState state = (ExecutionState)this.state_pool.borrowObject();
        state.init(clusterer);
        
        // Construct the MarkovGraphs for each Partition/Cluster using the Training Data Set
        this.generateMarkovGraphs(state, this.splits[SplitType.TRAINING.ordinal()]);
        
        // Generate the MarkovModels for the different partitions+clusters
        this.generateMarkovCostModels(state);
        
        // Now we need a mapping from TransactionIds -> ClusterIds
        // And then calculate the cost of using our cluster configuration to predict txn paths
        double total_c_cost = 0.0d;
        
        int c_counters[] = state.c_counters;
        int t_counters[] = state.t_counters;
        
//        Map<Pair<Long, Integer>, Histogram> key_to_cluster = new TreeMap<Pair<Long, Integer>, Histogram>(); 
//        Map<Integer, Histogram> cluster_to_key = new TreeMap<Integer, Histogram>();
        
        Instances validationData = this.splits[SplitType.VALIDATION.ordinal()];
        int validationCnt = this.split_counts[SplitType.VALIDATION.ordinal()];
        
        if (debug.val) LOG.debug(String.format("Estimating prediction rates of clusterer with %d transactions...", validationCnt));
        for (int i = 0; i < validationCnt; i++) {
            if (i > 0 && i % 1000 == 0) LOG.trace(String.format("TransactionTrace %d/%d", i, validationCnt));
            
            Instance inst = validationData.instance(i);
            long txn_id = FeatureUtil.getTransactionId(inst);
            TransactionTrace txn_trace = this.workload.getTransaction(txn_id);
            assert(txn_trace != null);
            Integer base_partition = this.getBasePartition(txn_trace);
            // Skip any txn that executes on a partition that we're not evaluating
            if (this.all_partitions.contains(base_partition) == false) continue;
            
            int c = (int)clusterer.clusterInstance(inst);

            // Debug Stuff
//            Pair<Long, Integer> key = Pair.of((Long)txn_trace.getParam(1), ((Object[])txn_trace.getParam(4)).length);
//            if (key_to_cluster.containsKey(key) == false) key_to_cluster.put(key, new Histogram());
//            key_to_cluster.get(key).put(c);
//            if (cluster_to_key.containsKey(c) == false) cluster_to_key.put(c, new Histogram());
//            cluster_to_key.get(c).put(key);
//            if (debug.val) LOG.debug(String.format("[%s, %s] => %d", , c));
            

            // Ok so now let's figure out what this mofo is going to do...
            PartitionSet partitions = this.getAllPartitions(txn_trace);
            boolean singlepartitioned = (partitions.size() == 1);
            t_counters[singlepartitioned ? 0 : 1]++;
            t_counters[2]++; // Total # of Txns
            
            // Estimate Clusterer MarkovGraphCost
            MarkovCostModel c_costmodel = state.costmodels_per_partition[base_partition.intValue()];
            double c_cost = 0.0;
            TxnToClusterMarkovGraphsContainer markovs = state.markovs_per_partition[base_partition.intValue()];
            markovs.addTransactionClusterXref(txn_id, c);
            MarkovGraph markov = markovs.get(c, catalog_proc);

            // Check that this is a cluster that we've seen before at this partition
            if (markov == null) {
                if (trace.val) LOG.warn(String.format("Txn #%d was mapped to never before seen Cluster #%d at partition %d", txn_id, c, base_partition));
                markov = markovs.getOrCreate(c, this.catalog_proc).initialize();
                markovs.addTransactionClusterXref(txn_id, c);
                // state.t_estimators_per_partition[base_partition.intValue()].processTransactionTrace(txn_trace);
                c_counters[2]++; // Unknown Clusters
            }
            c_cost = c_costmodel.estimateTransactionCost(catalogContext, txn_trace);
            if (c_cost > 0) {
                total_c_cost += c_cost;
                c_counters[singlepartitioned ? 0 : 1]++;
                
                // So that we can improve our predictions...
                markov.processTransaction(txn_trace, p_estimator);
                markov.calculateProbabilities(catalogContext.getAllPartitionIds());
                
//                if (c_counters[singlepartitioned ? 0 : 1] == 1) {
////                    MarkovPathEstimator.LOG.setLevel(Level.TRACE);
////                    MarkovPathEstimator estimator = new MarkovPathEstimator(markov, c_costmodel.getTransactionEstimator(c), base_partition, txn_trace.getParams());
////                    estimator.traverse(markov.getStartVertex());
////                    List<Vertex> e_path = estimator.getVisitPath();
//                    
//                    List<Vertex> e_path = c_costmodel.getLastEstimatedPath();
//                    List<Vertex> a_path = c_costmodel.getLastActualPath();
//                    for (int ii = 0, cnt = Math.max(e_path.size(), a_path.size()); ii < cnt; ii++) {
//                        Vertex e = (ii < e_path.size() ? e_path.get(ii) : null);
//                        Vertex a = (ii < a_path.size() ? a_path.get(ii) : null);
//                        String match = (e != null && e.equals(a) ? "" : "***");
//                        System.err.println(String.format("%-60s%-10s%s", e, match, a));
//                    } // FOR
//
//                    System.err.println("singlepartitioned = " + singlepartitioned);
//                    System.err.println("cost = " + c_cost);
//                    System.err.println("all_partitions             = " + all_partitions);
//                    System.err.println("actual partitions (R/W)    = " + c_costmodel.getReadWritePartitions(a_path));
//                    System.err.println("estimated partitions (R/W) = " + c_costmodel.getReadWritePartitions(e_path));
//                    System.err.println(txn_trace.debug(catalog_db));
//
//                    LOG.debug("Writing out mispredicated MarkovGraph paths [c_cost=" + c_cost + "]");
//                    GraphvizExport<Vertex, Edge> gv = MarkovUtil.exportGraphviz(markov, false, markov.getPath(c_costmodel.getLastEstimatedPath()));
//                    gv.highlightPath(markov.getPath(c_costmodel.getLastActualPath()), "blue");
//                    System.err.println("GRAPHVIZ: " + gv.writeToTempFile(catalog_proc, (singlepartitioned ? "single" : "multi")));
//                    System.err.println();
////                    System.exit(1);
//                    
//                    wrote_gv = true;
//                    // if (temp++ == 1) System.exit(1);
//                }
            }
        } // FOR
        
        if (debug.val) LOG.debug("Results: " + aset + "\n" + debugCounters(validationCnt, t_counters, c_counters, this.g_counters));
        
        this.state_pool.returnObject(state);
        
        aset.setCost(total_c_cost);
        return (clusterer);
    }

    /**
     * 
     * @param state
     * @param trainingData
     * @throws Exception
     */
    protected void generateMarkovGraphs(ExecutionState state, Instances trainingData) throws Exception {
        // Now iterate over validation set and construct Markov models
        // We have to know which field is our txn_id so that we can quickly access it
        int trainingCnt = trainingData.numInstances(); 
        if (trace.val) LOG.trace(String.format("Training MarkovGraphs using %d instances", trainingCnt));
        
        ObjectHistogram<Integer> cluster_h = new ObjectHistogram<Integer>();
        ObjectHistogram<Integer> partition_h = new ObjectHistogram<Integer>();
        for (int i = 0; i < trainingCnt; i++) {
            // Grab the Instance and throw it at the the clusterer to get the target cluster
            // The original data set is going to have the txn id that we need to grab 
            // the proper TransactionTrace record from the workload
            Instance inst = trainingData.instance(i);
            int c = (int)state.clusterer.clusterInstance(inst);
            cluster_h.put(c);
            
            long txn_id = Long.valueOf(inst.stringValue(FeatureExtractor.TXNID_ATTRIBUTE_IDX));
            TransactionTrace txn_trace = this.workload.getTransaction(txn_id);
            assert(txn_trace != null) : "Invalid TxnId #" + txn_id + "\n" + inst;

            // Figure out which base partition this txn would execute on
            // because we want divide the MarkovGraphContainers by the base partition
            int base_partition = this.p_estimator.getBasePartition(txn_trace);
            partition_h.put(base_partition);

            // Build up the MarkovGraph for this specific cluster
            MarkovGraphsContainer markovs = state.markovs_per_partition[base_partition];
            MarkovGraph markov = markovs.get(c, this.catalog_proc);
            if (markov == null) {
                markov = markovs.getOrCreate(c, this.catalog_proc).initialize();
                markovs.put(c, markov);
            }
            markov.processTransaction(txn_trace, this.p_estimator);
            
            state.clusters_per_partition[base_partition].put(c);
        } // FOR
        // if (trace.val) LOG.trace("Clusters per Partition:\n" + StringUtil.formatMaps(state.clusters_per_partition));
    }
    
    /**
     * 
     * @param state
     */
    protected void generateMarkovCostModels(final ExecutionState state) {
        // Now use the validation data set to figure out how well we are able to predict transaction
        // execution paths using the trained Markov graphs
        // We first need to construct a new costmodel and populate it with TransactionEstimators
        if (trace.val) LOG.trace("Constructing CLUSTER-BASED MarkovCostModels");
        
        // IMPORTANT: We run out of memory if we try to build the MarkovGraphs for all of the 
        // partitions+clusters. So instead we are going to randomly select some of the partitions to be used in the 
        // cost model estimation.
        final CountDownLatch costmodel_latch = new CountDownLatch(this.all_partitions.size());
        if (trace.val) LOG.trace(String.format("Generating MarkovGraphs for %d partitions", costmodel_latch.getCount()));
        
        for (final int partition : this.all_partitions) {
            final MarkovGraphsContainer markovs = state.markovs_per_partition[partition];
            if (trace.val) LOG.trace(String.format("Calculating Partition #%d probabilities for %d clusters", partition, markovs.size()));
            for (Entry<Integer, Map<Procedure, MarkovGraph>> e : markovs.entrySet()) {
                // if (debug.val) LOG.debug(String.format("Partition %d - Cluster %d", partition, i++));
                
                // Calculate the probabilities for each graph
                for (MarkovGraph markov : e.getValue().values()) {
                    markov.calculateProbabilities(catalogContext.getAllPartitionIds());
                } // FOR
            } // FOR
            if (trace.val) LOG.trace(String.format("Finished processing MarkovGraphs for Partition #%d [count=%d]", partition, costmodel_latch.getCount()));
            costmodel_latch.countDown();
            // this.generate_threadPool.execute(r);
        } // FOR
//        // Wait until everyone finishes
//        try {
//            costmodel_latch.await();
//        } catch (Exception ex) {
//            throw new RuntimeException(ex);
//        }
    }
    
    /**
     * 
     * @param trainingData
     * @param round
     * @throws Exception
     */
    protected AbstractClusterer createClusterer(MarkovAttributeSet aset, Instances trainingData) throws Exception {
        if (trace.val) LOG.trace(String.format("Clustering %d %s instances with %d attributes", trainingData.numInstances(), CatalogUtil.getDisplayName(catalog_proc), aset.size()));
        
        // Create the filter we need so that we only include the attributes in the given MarkovAttributeSet
        Filter filter = aset.createFilter(trainingData);
        
        // Using our training set to build the clusterer
        int seed = this.rand.nextInt(); 
//        SimpleKMeans inner_clusterer = new SimpleKMeans();
        EM inner_clusterer = new EM();
        String options[] = {
            "-N", Integer.toString(1000), // num_partitions),
            "-S", Integer.toString(seed),
            "-I", Integer.toString(100),
            
        };
        inner_clusterer.setOptions(options);
        
        FilteredClusterer filtered_clusterer = new FilteredClusterer();
        filtered_clusterer.setFilter(filter);
        filtered_clusterer.setClusterer(inner_clusterer);
        
        AbstractClusterer clusterer = filtered_clusterer;
        clusterer.buildClusterer(trainingData);
        
        return (clusterer);
    }
    
    protected Classifier generateDecisionTree(AbstractClusterer clusterer, MarkovAttributeSet aset, Instances data) throws Exception {
        // We need to create a new Attribute that has the ClusterId
        Instances newData = data; // new Instances(data);
        newData.insertAttributeAt(new Attribute("ClusterId"), newData.numAttributes());
        Attribute cluster_attr = newData.attribute(newData.numAttributes()-1);
        assert(cluster_attr != null);
        assert(cluster_attr.index() > 0);
        newData.setClass(cluster_attr);
        
        // We will then tell the Classifier to predict that ClusterId based on the MarkovAttributeSet
        ObjectHistogram<Integer> cluster_h = new ObjectHistogram<Integer>();
        for (int i = 0, cnt = newData.numInstances(); i < cnt; i++) {
            // Grab the Instance and throw it at the the clusterer to get the target cluster
            Instance inst = newData.instance(i);
            int c = (int)clusterer.clusterInstance(inst);
            inst.setClassValue(c);
            cluster_h.put(c);
        } // FOR
        System.err.println("Number of Elements: " + cluster_h.getValueCount());
        System.err.println(cluster_h);

        NumericToNominal filter = new NumericToNominal();
        filter.setInputFormat(newData);
        newData = Filter.useFilter(newData, filter);
        
        String output = this.catalog_proc.getName() + "-labeled.arff";
        FileUtil.writeStringToFile(output, newData.toString());
        LOG.info("Wrote labeled data set to " + output);
        
        // Decision Tree
        J48 j48 = new J48();
        String options[] = {
            "-S", Integer.toString(this.rand.nextInt()),
            
        };
        j48.setOptions(options);

        // Make sure we add the ClusterId attribute to a new MarkovAttributeSet so that
        // we can tell the Classifier to classify that!
        FilteredClassifier fc = new FilteredClassifier();
        MarkovAttributeSet classifier_aset = new MarkovAttributeSet(aset);
        classifier_aset.add(cluster_attr);
        fc.setFilter(classifier_aset.createFilter(newData));
        fc.setClassifier(j48);
        
        // Bombs away!
        fc.buildClassifier(newData);
        
        return (fc);
    }
    
    
    /**
     * Helper method to convet Feature keys to Attributes
     * @param data
     * @param prefixes
     * @return
     */
    public static Set<Attribute> prefix2attributes(Instances data, String...prefixes) {
        Set<Attribute> attributes = new ListOrderedSet<Attribute>();
        for (String key : prefixes) {
            Attribute attribute = data.attribute(key);
            assert(attribute != null) : "Invalid Attribute key '" + key + "'";
            attributes.add(attribute);
        } // FOR
        return (attributes);
    }

    protected static String debugCounters(int validationCnt, int t_counters[], int c_counters[], int g_counters[]) {
        int values[][] = new int[][]{
            t_counters,
            c_counters,
            g_counters,
        };
        String labels[] = {
            "Prediction Result",
            "Single-Partition",
            "Multi-Partition",
            "Unknown Clusters",
        };
        int totals[] = {
            t_counters[2],
            t_counters[0],
            t_counters[1],
            t_counters[2],
        };

        final int total_txns = values[0][2];
        final int value_len = Integer.toString(total_txns).length();
        final String f = "%" + value_len + "d / %" + value_len + "d [%.03f]";
        final ListOrderedMap<?, ?> maps[] = new ListOrderedMap<?, ?>[values.length];
        for (int i = 0; i < values.length; i++) {
            ListOrderedMap<String, String> m = new ListOrderedMap<String, String>();
            int singlep = values[i][0];
            int multip = values[i][1];
            int missed = values[i][2];
            
            if (i == 0) {
                m.put("# of Evaluated Transactions", String.format(f, total_txns, validationCnt, (total_txns / (double)validationCnt)));
            } else {
                String prefix = (i == 1 ? "Clusterer" : "Global");
                int inner[] = new int[]{
                    singlep + multip,
                    singlep,
                    multip,
                    total_txns - missed,
                };
                for (int ii = 0; ii < inner.length; ii++) {
                    String value = String.format(f, totals[ii] - inner[ii],                // Count
                                                    totals[ii],                            // Total
                                                    1.0 - (inner[ii] / (double)totals[ii]) // Percentage
                    );
                    m.put(prefix + " " + labels[ii], value);
                } // FOR
            }
            maps[i] = m;
        } // FOR
        
        return (StringUtil.formatMaps(maps));
    }
    
    /**
     * Main!
     * @param vargs
     * @throws Exception
     */
    public static void main(String[] vargs) throws Exception {
        ArgumentsParser args = ArgumentsParser.load(vargs);
        args.require(
            ArgumentsParser.PARAM_CATALOG,
            ArgumentsParser.PARAM_WORKLOAD,
            ArgumentsParser.PARAM_MAPPINGS
        );
        
        // Number of threads
        int num_threads = FeatureClusterer.DEFAULT_NUM_THREADS;
        if (args.hasIntParam(ArgumentsParser.PARAM_MARKOV_THREADS)) {
            num_threads = args.getIntParam(ArgumentsParser.PARAM_MARKOV_THREADS);
        }
        
        // Get the procedure we're suppose to investigate
        String proc_name = args.getOptParam(0);
        Procedure catalog_proc = args.catalog_db.getProcedures().getIgnoreCase(proc_name);
        assert(catalog_proc != null) : proc_name;
        
        // And our Weka data file
//        File arff_path = new File(args.getOptParam(1));
//        assert(arff_path.exists()) : arff_path.getAbsolutePath();
//        BufferedReader reader = new BufferedReader(new FileReader(arff_path));
//        Instances data = new Instances(reader);
//        reader.close();
//        data = new Instances(data, 0, args.workload.getTransactionCount());
        
        Instances data = null;
        {
            // Hopefully this will get garbage collected if we put it here...
            FeatureExtractor fextractor = new FeatureExtractor(args.catalogContext);
            Map<Procedure, FeatureSet> fsets = fextractor.calculate(args.workload);
            FeatureSet fset = fsets.get(catalog_proc);
            assert(fset != null) : "Failed to get FeatureSet for " + catalog_proc;
            data = fset.export(catalog_proc.getName());
        }
        assert(data != null);
        assert(args.workload.getTransactionCount() == data.numInstances());
        
        PartitionSet partitions = null;
        if (args.hasParam(ArgumentsParser.PARAM_WORKLOAD_RANDOM_PARTITIONS)) {
            PartitionEstimator p_estimator = new PartitionEstimator(args.catalogContext);
            final ObjectHistogram<Integer> h = new ObjectHistogram<Integer>();
            for (TransactionTrace txn_trace : args.workload.getTransactions()) {
                int base_partition = p_estimator.getBasePartition(txn_trace);
                h.put(base_partition);
            } // FOR
//            System.err.println("# OF PARTITIONS: " + h.getValueCount());
//            h.setKeepZeroEntries(true);
//            for (Integer p : CatalogUtil.getAllPartitionIds(args.catalog_db)) {
//                if (h.contains(p) == false) h.put(p, 0);
//            }
//            System.err.println(h);
//            System.exit(1);
//            
            partitions = new PartitionSet(h.values());
        } else {
            partitions = args.catalogContext.getAllPartitionIds();    
        }
        FeatureClusterer fclusterer = new FeatureClusterer(args.catalogContext,
                                                           catalog_proc,
                                                           args.workload,
                                                           partitions,
                                                           num_threads);
        // Update split configuration variables
        for (SplitType type : SplitType.values()) {
            String param_name = String.format("%s.%s", ArgumentsParser.PARAM_MARKOV_SPLIT, type.name());
            if (args.hasDoubleParam(param_name) == false) continue;
            double percentage = args.getDoubleParam(param_name);
            fclusterer.setSplitPercentage(type, percentage);
        } // FOR
        if (args.hasDoubleParam(ArgumentsParser.PARAM_MARKOV_TOPK)) {
            fclusterer.setAttributeTopK(args.getDoubleParam(ArgumentsParser.PARAM_MARKOV_TOPK));
        }
        if (args.hasIntParam(ArgumentsParser.PARAM_MARKOV_ROUNDS)) {
            fclusterer.setNumRounds(args.getIntParam(ArgumentsParser.PARAM_MARKOV_ROUNDS));
        }

//      MarkovAttributeSet aset = fclusterer.calculate(data);
        
        // HACK
        Set<Attribute> attributes = FeatureClusterer.prefix2attributes(data,
            "ParamArrayLength-04"
//            "ParamHashPartition-01"
        );
        MarkovAttributeSet aset = new MarkovAttributeSet(attributes);
        Map<Integer, MarkovGraphsContainer> markovs = fclusterer.constructMarkovModels(aset, data);
        
        File output = new File(catalog_proc.getName() + ".markovs");
        MarkovGraphsContainerUtil.save(markovs, output);
        
//        fclusterer.calculateGlobalCost();
//        AbstractClusterer clusterer = fclusterer.calculateAttributeSetCost(aset);
//        fclusterer.generateDecisionTree(clusterer, aset, data);
//        
//        System.err.println(aset + "\nCost: " + aset.getCost());
        fclusterer.cleanup();
    }
    
}
