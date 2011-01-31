package edu.brown.markov;

import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.collections15.map.ListOrderedMap;
import org.apache.commons.collections15.set.ListOrderedSet;
import org.apache.log4j.Logger;
import org.voltdb.catalog.Database;
import org.voltdb.catalog.Procedure;
import org.voltdb.utils.Pair;

import weka.clusterers.AbstractClusterer;
import weka.clusterers.EM;
import weka.clusterers.FilteredClusterer;
import weka.clusterers.SimpleKMeans;
import weka.core.Attribute;
import weka.core.Instance;
import weka.core.Instances;
import weka.filters.Filter;
import weka.filters.unsupervised.attribute.Remove;
import edu.brown.catalog.CatalogUtil;
import edu.brown.correlations.ParameterCorrelations;
import edu.brown.costmodel.MarkovCostModel;
import edu.brown.markov.features.BasePartitionFeature;
import edu.brown.markov.features.FeatureUtil;
import edu.brown.markov.features.ParamArrayLengthFeature;
import edu.brown.markov.features.ParamNumericValuesFeature;
import edu.brown.statistics.Histogram;
import edu.brown.utils.ArgumentsParser;
import edu.brown.utils.CollectionUtil;
import edu.brown.utils.LoggerUtil;
import edu.brown.utils.PartitionEstimator;
import edu.brown.utils.StringUtil;
import edu.brown.utils.ThreadUtil;
import edu.brown.utils.UniqueCombinationIterator;
import edu.brown.utils.LoggerUtil.LoggerBoolean;
import edu.brown.workload.TransactionTrace;
import edu.brown.workload.Workload;

/**
 * 
 * @author pavlo
 */
public class FeatureClusterer {
    private static final Logger LOG = Logger.getLogger(FeatureClusterer.class);
    private final static LoggerBoolean debug = new LoggerBoolean(LOG.isDebugEnabled());
    private final static LoggerBoolean trace = new LoggerBoolean(LOG.isTraceEnabled());
    static {
        LoggerUtil.attachObserver(LOG, debug, trace);
    }
    
    // ----------------------------------------------------------------------------
    // DEFAULT CONFIGURATION VALUES
    // ----------------------------------------------------------------------------
    
    // Training Workload Percentage
    private static double TRAINING_SET_PERCENTAGE = 0.30;
    
    // Validation Workload Percentage 
    private static double VALIDATION_SET_PERCENTAGE = 0.30;

    // Testing Workload Percentage
    private static double TESTING_SET_PERCENTAGE = 0.40;
    
    // What percentage of the partitions should be evaluated when estimating the clusterer cost
    // This is needed when have a large number of partitions because the number of probabilities
    // per vertex will get quite large.
    private static double PARTITION_EVALUATION_FACTOR = 0.35;
    
    // For each search round, we will only propagate the attributes found this these top-k AttibuteSets 
    private static double ATTRIBUTESET_TOP_K = 0.10;
    
    // Number of threads to use per thread pool
    private static final int NUM_THREADS_PER_POOL = 5;
    
    // Number of search rounds in findBestAttributeSet
    private static int NUM_ROUNDS = 10;
    
    // ----------------------------------------------------------------------------
    // DATA MEMBERS
    // ----------------------------------------------------------------------------
    
    private final Database catalog_db;
    private final Procedure catalog_proc;
    private final Workload workload;
    private final ParameterCorrelations correlations;
    private final PartitionEstimator p_estimator;
    private final Random rand = new Random(0);
    private final Collection<Integer> all_partitions;
    
    // We have one thread pool for findBestAttributeSet and one for createAttributeSet
    private final ExecutorService createAttribute_threadpool;
    
    /**
     * A set of attributes and their cost
     */
    protected static class AttributeSet extends ListOrderedSet<Attribute> implements Comparable<AttributeSet> {
        private static final long serialVersionUID = 1L;
        private Double cost;

        public AttributeSet(Set<Attribute> items) {
            super(items);
        }
        
        public AttributeSet(Attribute...items) {
            super((Set<Attribute>)CollectionUtil.addAll(new HashSet<Attribute>(), items));
        }
        
        protected AttributeSet(Instances data, Collection<Integer> idxs) {
            for (Integer i : idxs) {
                this.add(data.attribute(i));
            } // FOR
        }
        
        public Filter createFilter(Instances data) throws Exception {
            Set<Integer> indexes = new HashSet<Integer>();
            for (int i = 0, cnt = this.size(); i < cnt; i++) {
                indexes.add(this.get(i).index());
            } // FOR
            
            SortedSet<Integer> to_remove = new TreeSet<Integer>(); 
            for (int i = 0, cnt = data.numAttributes(); i < cnt; i++) {
                if (indexes.contains(i) == false) {
                    to_remove.add(i+1);
                }
            } // FOR
            
            Remove filter = new Remove();
            filter.setInputFormat(data);
            String options[] = { "-R", StringUtil.join(",", to_remove) };
            filter.setOptions(options);
            return (filter);
        }
//        
//        public Instances copyData(Instances data) throws Exception {
//            Set<Integer> indexes = new HashSet<Integer>();
//            for (int i = 0, cnt = this.size(); i < cnt; i++) {
//                indexes.add(this.get(i).index());
//            } // FOR
//            
//            SortedSet<Integer> to_remove = new TreeSet<Integer>(); 
//            for (int i = 0, cnt = data.numAttributes(); i < cnt; i++) {
//                if (indexes.contains(i) == false) {
//                    to_remove.add(i+1);
//                }
//            } // FOR
//            
//            Remove filter = new Remove();
//            filter.setInputFormat(data);
//            filter.setAttributeIndices(StringUtil.join(",", to_remove));
//            for (int i = 0, cnt = data.numInstances(); i < cnt; i++) {
//                filter.input(data.instance(i));
//            } // FOR
//            filter.batchFinished();
//            
//            Instances newData = filter.getOutputFormat();
//            Instance processed;
//            while ((processed = filter.output()) != null) {
//                newData.add(processed);
//            } // WHILE
//            return (newData);
//        }
        
        public Double getCost() {
            return (this.cost);
        }
        public void setCost(Double cost) {
            this.cost = cost;
        }
        @Override
        public int compareTo(AttributeSet o) {
            if (this.cost != o.cost) {
                return (this.cost != null ? this.cost.compareTo(o.cost) : o.cost.compareTo(this.cost));
            } else if (this.size() != o.size()) {
                return (this.size() - o.size());
            } else if (this.containsAll(o)) {
                return (0);
            }
            for (int i = 0, cnt = this.size(); i < cnt; i++) {
                int idx0 = this.get(i).index();
                int idx1 = o.get(i).index();
                if (idx0 != idx1) return (idx0 - idx1);
            } // FOR
            return (0);
        }
        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder();
            String add = "[";
            for (Attribute a : this) {
                sb.append(add).append(a.name());
                add = ", ";
            }
            sb.append("]");
            return sb.toString();
        }
    }
    
    private class ExecutionState {

        /**
         * 
         */
        private AbstractClusterer clusterer;
        
        /**
         * We want to always split the MarkovGraphContainers by base partition, since we already know
         * that this is going to be the best predictor
         */
        final Map<Integer, MarkovGraphsContainer> markovs_per_partition = new HashMap<Integer, MarkovGraphsContainer>();
        
        /**
         * We also maintain a "global" MarkovGraphContainer that consumes all transactions
         * We will use to compare whether our cluster-specific models do better than the global one
         */
        final MarkovGraphsContainer global_markov = new MarkovGraphsContainer();
        
        /**
         * Then we have a costmodel for each ClusterId 
         */
        final Map<Integer, MarkovCostModel> clusterer_costmodels = new HashMap<Integer, MarkovCostModel>();
        
        /**
         *
         */
        final MarkovCostModel global_costmodel = new MarkovCostModel(catalog_db, p_estimator);
        
        /**
         * Clusters Per Partition
         */
        Map<Integer, Histogram> clusters_per_partition = new HashMap<Integer, Histogram>();
        
        
        public ExecutionState() {
            for (Integer p : FeatureClusterer.this.all_partitions) {
                this.clusters_per_partition.put(p, new Histogram());
                this.markovs_per_partition.put(p, new MarkovGraphsContainer());
                this.global_markov.create(p, FeatureClusterer.this.catalog_proc).initialize();
                
                MarkovCostModel costmodel = new MarkovCostModel(FeatureClusterer.this.catalog_db, FeatureClusterer.this.p_estimator);
                costmodel.setTransactionClusterMapping(true);
                this.clusterer_costmodels.put(p, costmodel);
            } // FOR
        }
        
        public void init(AbstractClusterer clusterer) {
            this.clusterer = clusterer;
        }

        public void finished() {
            this.global_markov.clear();
            
            for (MarkovGraphsContainer mgc : this.markovs_per_partition.values()) {
                mgc.clear();
            } // FOR
            for (Histogram h : this.clusters_per_partition.values()) {
                h.clear();
            } // FOR
            for (MarkovCostModel mcm : this.clusterer_costmodels.values()) {
                mcm.clear();
            } // FOR
        }
    }
    
    /**
     * 
     * @param catalog_db
     * @param workload
     * @param correlations
     * @param all_partitions
     */
    public FeatureClusterer(Procedure catalog_proc, Workload workload, ParameterCorrelations correlations, Collection<Integer> all_partitions) {
        this.catalog_proc = catalog_proc;
        this.catalog_db = CatalogUtil.getDatabase(catalog_proc);
        this.workload = workload;
        this.correlations = correlations;
        this.p_estimator = new PartitionEstimator(catalog_db);
        this.all_partitions = all_partitions;
        this.createAttribute_threadpool = Executors.newFixedThreadPool(NUM_THREADS_PER_POOL);
    }
    
    /**
     * Constructor
     * @param catalog_db
     */
    public FeatureClusterer(Procedure catalog_proc, Workload workload, ParameterCorrelations correlations) {
        this(catalog_proc, workload, correlations, CatalogUtil.getAllPartitionIds(catalog_proc));
    }
    
    protected final void cleanup() {
        this.createAttribute_threadpool.shutdownNow();
    }
    
    /**
     * 
     * @param data
     * @return
     */
    protected Instances[] splitWorkload(Instances data) {
        Instances split[] = new Instances[3];
        int all_cnt = data.numInstances();
        
        // Training Data
        int training_cnt = (int)Math.round(all_cnt * TRAINING_SET_PERCENTAGE);
        split[0] = new Instances(data, 0, training_cnt);
        
        // Validation Data
        int validation_cnt = (int)Math.round(all_cnt * VALIDATION_SET_PERCENTAGE);
        split[1] = new Instances(data, training_cnt, validation_cnt);
        
        // Testing Data
        int testing_cnt = (int)Math.round(all_cnt * TESTING_SET_PERCENTAGE);
        split[2] = new Instances(data, training_cnt+validation_cnt, testing_cnt);
        
        return (split);
    }

    /**
     * 
     * @param fset
     * @param data
     * @param catalog_proc
     * @throws Exception
     */
    @SuppressWarnings("unchecked")
    protected void calculate(final Instances data) throws Exception {
        AttributeSet best_set = null;
        
        // Get the list of all the attributes that we are going to want to try to cluster on
        // We want to always remove the first attribute because that's the TransactionId
        ListOrderedSet<Attribute> all_attributes = (ListOrderedSet<Attribute>)CollectionUtil.addAll(new ListOrderedSet<Attribute>(), data.enumerateAttributes());
        all_attributes.remove(0);

        // Split the input data set into separate data sets
        Instances[] workloads = this.splitWorkload(data);
        final Instances trainingData = workloads[0];
        int trainingCnt = trainingData.numInstances();
        
        final Instances validationData = workloads[1];
        int validationCnt = validationData.numInstances();
        
        final Instances testingData = workloads[2];
        int testingCnt = testingData.numInstances();
        
        // List of all AttributeSets ever created
        SortedSet<AttributeSet> all_asets = new TreeSet<AttributeSet>();

        Integer base_partition_idx = data.attribute(FeatureUtil.getFeatureKeyPrefix(BasePartitionFeature.class)).index();
        assert(base_partition_idx != null);
        Attribute base_partition_attr = trainingData.attribute(base_partition_idx);
        assert(base_partition_attr != null);
        
        int round = 0;
        while (round++ < NUM_ROUNDS) {
            if (debug.get()) {
                if (round == 1) {
                    LOG.debug("# of Training Instances:    " + trainingCnt);
                    LOG.debug("# of Validation Instances:  " + validationCnt);
                    LOG.debug("# of Testing Instances:     " + testingCnt);
                }
                
                Map<String, Object> m = new ListOrderedMap<String, Object>();
                m.put("Round #", String.format("%02d", round));
                m.put("Best Set", best_set);
                m.put("Best Cost", (best_set != null ? best_set.getCost() : null));
                LOG.debug("\n" + StringUtil.formatMaps(":", true, true, m));
            }

            // The AttributeSets created in this round
            final SortedSet<AttributeSet> round_asets = new TreeSet<AttributeSet>();

            int aset_ctr = 0;
            for (final Set<Attribute> s : UniqueCombinationIterator.factory(all_attributes, round)) {
                AttributeSet aset = null;
                try {
                    aset = this.createAttributeSet(s, workloads);
                } catch (Exception ex) {
                    LOG.fatal("Failed to calculate AttributeSet cost for " + s, ex);
                    throw new RuntimeException(ex);
                }
                if (debug.get()) LOG.debug(String.format("[%03d] Attributes%s => %.03f", aset_ctr++, aset.toString(), aset.getCost()));
                round_asets.add(aset);        
            } // WHILE (AttributeSet)
            
            all_asets.addAll(round_asets);
            
            // Now figure out what the top-k AttributeSets from this round
            // For now we'll explode out all of the attributes that they contain and throw that into a set
            // of candidate attributes for the next round
            all_attributes.clear();
            int top_k = (int)Math.round(round_asets.size() * ATTRIBUTESET_TOP_K);
            for (AttributeSet aset : round_asets) {
                all_attributes.addAll(aset);
                LOG.info(String.format("%.03f\t%s", aset.getCost(), aset.toString()));
                if (top_k-- == 0) break;
            } // FOR
            // if (round == 1) all_attributes.add(data.attribute(1));
            
            AttributeSet round_best = round_asets.first();
            if (best_set == null || round_best.getCost() < best_set.getCost()) {
                best_set = round_best;
            }
            
            LOG.info(String.format("Next Round Attributes [size=%d]: %s", all_attributes.size(), new AttributeSet(all_attributes)));
        } // WHILE (round)
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
    protected AttributeSet createAttributeSet(Set<Attribute> attributes, final Instances data[]) throws Exception {
        final AttributeSet aset = new AttributeSet(attributes);
        
        Instances trainingData = data[0];
        final int trainingCnt = trainingData.numInstances();
        
        Instances validationData = data[1];
        final int validationCnt = validationData.numInstances();
        
        Instances testingData = data[2];
        final int testingCnt = testingData.numInstances();
        
        // Build our clusterer
        if (debug.get()) LOG.debug(String.format("Training Clusterer [attributes=%d, training=%d, validation=%d, partitions=%d]", aset.size(), trainingCnt, validationCnt, this.all_partitions.size()));
        AbstractClusterer clusterer = this.createClusterer(trainingData, aset);
        
        ExecutionState state = new ExecutionState();
        state.init(clusterer);
        
        // Construct the MarkovGraphs for each Partition/Cluster using the Training Data Set
        this.constructMarkovModels(state, trainingData);
        
        //
        this.buildCostModels(state);


        
        // DEBUG
        // MarkovUtil.save(markovs, "/tmp/" + catalog_proc.getName() + ".markovs");
        
        // Now we need a mapping from TransactionIds -> ClusterIds
        // And then calculate the cost of using our cluster configuration to predict txn paths
        double total_c_cost = 0.0d;
        double total_g_cost = 0.0d;
        int c_counters[] = new int[] {
            0,      // Single-P
            0,      // Multi-P
            0,      // Known Clusters
        };
        int g_counters[] = new int[] {
            0,      // Single-P
            0,      // Multi-P
            0,      // Known Clusters
        };
        int t_counters[] = new int[] {
            0,      // Single-P
            0,      // Multi-P
            0,      // Total # of Txns
        };
        
//        Map<Pair<Long, Integer>, Histogram> key_to_cluster = new TreeMap<Pair<Long, Integer>, Histogram>(); 
//        Map<Integer, Histogram> cluster_to_key = new TreeMap<Integer, Histogram>();
        
        if (debug.get()) LOG.debug(String.format("Estimating prediction rates of clusterer with %d transactions...", validationCnt));
        for (int i = 0; i < validationCnt; i++) {
            if (i > 0 && i % 1000 == 0) LOG.debug(String.format("TransactionTrace %d/%d", i, validationCnt));
            
            Instance inst = validationData.instance(i);
            long txn_id = Long.valueOf(inst.stringValue(FeatureExtractor.TXNID_ATTRIBUTE_IDX));
            TransactionTrace txn_trace = this.workload.getTransaction(txn_id);
            assert(txn_trace != null);
            int base_partition = this.p_estimator.getBasePartition(txn_trace);
            // Skip any txn that executes on a partition that we're not evaluating
            if (this.all_partitions.contains(base_partition) == false) continue;
            
            int c = (int)clusterer.clusterInstance(inst);

            // Debug Stuff
//            Pair<Long, Integer> key = Pair.of((Long)txn_trace.getParam(1), ((Object[])txn_trace.getParam(4)).length);
//            if (key_to_cluster.containsKey(key) == false) key_to_cluster.put(key, new Histogram());
//            key_to_cluster.get(key).put(c);
//            if (cluster_to_key.containsKey(c) == false) cluster_to_key.put(c, new Histogram());
//            cluster_to_key.get(c).put(key);
//            if (debug.get()) LOG.debug(String.format("[%s, %s] => %d", , c));
            

            // Ok so now let's figure out what this mofo is going to do...
            Set<Integer> all_partitions = this.p_estimator.getAllPartitions(txn_trace);
            boolean singlepartitioned = (all_partitions.size() == 1);
            t_counters[singlepartitioned ? 0 : 1]++;
            t_counters[2]++;
            
            // Estimate Global MarkovGraph Cost
            state.global_costmodel.addTransactionClusterXref(txn_id, base_partition);
            double g_cost = state.global_costmodel.estimateTransactionCost(this.catalog_db, txn_trace);
            if (g_cost > 0) {
                total_g_cost += g_cost;
                g_counters[singlepartitioned ? 0 : 1]++;
            }
            
            // Estimate Clusterer MarkovGraphCost
            MarkovCostModel c_costmodel = state.clusterer_costmodels.get(base_partition);
            double c_cost = 0.0;
            MarkovGraphsContainer markovs = state.markovs_per_partition.get(base_partition);
            MarkovGraph markov = markovs.get(c, catalog_proc);

            // Check that this is a cluster that we've seen before at this partition
            if (markov == null) {
                if (trace.get()) LOG.warn(String.format("Txn #%d was mapped to never before seen Cluster #%d at partition %d", txn_id, c, base_partition));
                markov = markovs.create(c, this.catalog_proc).initialize();
                
                TransactionEstimator t_estimator = new TransactionEstimator(this.p_estimator, this.correlations);
                t_estimator.addMarkovGraph(this.catalog_proc, markov);
                t_estimator.processTransactionTrace(txn_trace);
                c_costmodel.addTransactionEstimator(c, t_estimator);
                c_counters[2]++;
            }
            c_costmodel.addTransactionClusterXref(txn_id, c);
            c_cost = c_costmodel.estimateTransactionCost(this.catalog_db, txn_trace);
            if (c_cost > 0) {
                total_c_cost += c_cost;
                int idx = c_counters[singlepartitioned ? 0 : 1]++;
                
//                if (idx == 1) {
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
//                        String match = (e != null && e.equals(a) ? "" : "XXX");
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
        
        if (debug.get()) {
//            LOG.debug("Keys to Clusters:\n" + StringUtil.formatMaps(key_to_cluster));
//            LOG.debug("Clusters to Keys:\n" + StringUtil.formatMaps(cluster_to_key));
            
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
            
            String f = "%d/%d [%.03f]";
            int total_txns = values[0][2]; 
            ListOrderedMap<?, ?> maps[] = new ListOrderedMap<?, ?>[values.length];
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
                        m.put(prefix + " " + labels[ii], String.format(f, totals[ii] - inner[ii], totals[ii], 1.0 - (inner[ii] / (double)totals[ii])));
                    } // FOR
                }
                maps[i] = m;
            } // FOR
            LOG.debug("Results: " + aset + "\n" + StringUtil.formatMaps(maps));
        }
        
        aset.setCost(total_c_cost);
        return (aset);
    }

    /**
     * 
     * @param state
     * @param trainingData
     * @throws Exception
     */
    protected void constructMarkovModels(final ExecutionState state, Instances trainingData) throws Exception {
        // Now iterate over validation set and construct Markov models
        // We have to know which field is our txn_id so that we can quickly access it
        Histogram cluster_h = new Histogram();
        Histogram partition_h = new Histogram();
        for (int i = 0, cnt = trainingData.numInstances(); i < cnt; i++) {
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
            // Build up the MarkovGraph for this cluster
            int base_partition = this.p_estimator.getBasePartition(txn_trace);
            partition_h.put(base_partition);
            
            MarkovGraphsContainer markovs = state.markovs_per_partition.get(base_partition);
            MarkovGraph markov = markovs.get(c, this.catalog_proc);
            if (markov == null) {
                markov = markovs.create(c, this.catalog_proc).initialize();
                markovs.put(c, markov);
            }
            markov.processTransaction(txn_trace, this.p_estimator);
            state.global_markov.get(base_partition, this.catalog_proc).processTransaction(txn_trace, this.p_estimator);
            state.clusters_per_partition.get(base_partition).put(c);
        } // FOR
        if (trace.get()) LOG.trace("Clusters per Partition:\n" + StringUtil.formatMaps(state.clusters_per_partition));
    }
    
    /**
     * 
     * @param state
     */
    protected void buildCostModels(final ExecutionState state) {
        // Now use the validation data set to figure out how well we are able to predict transaction
        // execution paths using the trained Markov graphs
        // We first need to construct a new costmodel and populate it with TransactionEstimators
        if (debug.get()) LOG.debug("Constructing CLUSTER-BASED MarkovCostModels");
        
        // IMPORTANT: We run out of memory if we try to build the MarkovGraphs for all of the 
        // partitions+clusters. So instead we are going to randomly select some of the partitions to be used in the 
        // cost model estimation.
        final CountDownLatch costmodel_latch = new CountDownLatch(this.all_partitions.size() + 1);
        if (debug.get()) LOG.debug(String.format("Generating MarkovGraphs for %d partitions", costmodel_latch.getCount()));
        
        for (final Integer partition : this.all_partitions) {
            final MarkovGraphsContainer markovs = state.markovs_per_partition.get(partition);
            final MarkovCostModel costmodel = state.clusterer_costmodels.get(partition);
            Runnable r = new Runnable() {   
                @Override
                public void run() {
                    if (trace.get()) LOG.trace(String.format("Calculating Partition #%d probabilities for %d clusters", partition, markovs.size()));
                    for (Entry<Integer, Map<Procedure, MarkovGraph>> e : markovs.entrySet()) {
                        // if (debug.get()) LOG.debug(String.format("Partition %d - Cluster %d", partition, i++));
                        
                        // Calculate the probabilities for each graph
                        for (MarkovGraph markov : e.getValue().values()) {
                            markov.calculateProbabilities();
                        } // FOR
                        
                        TransactionEstimator t_estimator = new TransactionEstimator(p_estimator, correlations);
                        t_estimator.addMarkovGraphs(e.getValue());
                        costmodel.addTransactionEstimator(e.getKey(), t_estimator);
                    } // FOR
                    if (debug.get()) LOG.debug(String.format("Finished processing MarkovGraphs for Partition #%d [count=%d]", partition, costmodel_latch.getCount()));
                    costmodel_latch.countDown();
                }
            };
            this.createAttribute_threadpool.execute(r);
        } // FOR

        if (debug.get()) LOG.debug("Constructing GLOBAL MarkovCostModel");
        
        this.createAttribute_threadpool.execute(new Runnable() {
            @Override
            public void run() {
                state.global_costmodel.setTransactionClusterMapping(true);
                for (Integer partition : FeatureClusterer.this.all_partitions) {
                    state.global_markov.get(partition, catalog_proc).calculateProbabilities();
                    TransactionEstimator t_estimator = new TransactionEstimator(p_estimator, correlations);
                    t_estimator.addMarkovGraphs(state.global_markov.get(partition));
                    state.global_costmodel.addTransactionEstimator(partition, t_estimator);
                } // FOR
                if (debug.get()) LOG.debug(String.format("Finished initializing GLOBAL MarkovCostModel [count=%d]", costmodel_latch.getCount()));
                costmodel_latch.countDown();
            }
        });

        // Wait until everyone finishes
        try {
            costmodel_latch.await();
        } catch (Exception ex) {
            LOG.fatal(ex);
            System.exit(1);
        }
    }
    
    /**
     * 
     * @param data
     * @param round
     * @throws Exception
     */
    protected AbstractClusterer createClusterer(Instances data, AttributeSet aset) throws Exception {
        if (trace.get()) LOG.trace(String.format("Clustering %d %s instances with %d attributes", data.numInstances(), CatalogUtil.getDisplayName(catalog_proc), aset.size()));
        
        // Create the filter we need so that we only include the attributes in the given AttributeSet
        Filter filter = aset.createFilter(data);
        
        // Using our training set to build the clusterer
        int seed = this.rand.nextInt(); 
        SimpleKMeans inner_clusterer = new SimpleKMeans();
//        EM inner_clusterer = new EM();
        String options[] = {
            "-N", Integer.toString(1000), // num_partitions),
            "-S", Integer.toString(seed),
        };
        inner_clusterer.setOptions(options);
//        kmeans_clusterer.setNumClusters(num_partitions);
//        kmeans_clusterer.setSeed(seed);
        
        FilteredClusterer filtered_clusterer = new FilteredClusterer();
        filtered_clusterer.setFilter(filter);
        filtered_clusterer.setClusterer(inner_clusterer);
        
        AbstractClusterer clusterer = filtered_clusterer; // kmeans_clusterer;
//        clusterer.buildClusterer(Filter.useFilter(data, filter));
        clusterer.buildClusterer(data);
        
        return (clusterer);
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
            ArgumentsParser.PARAM_CORRELATIONS
        );
        
        // Update static configuration variables
        if (args.hasDoubleParam(ArgumentsParser.PARAM_MARKOV_WORKLOAD_SPLIT)) {
            FeatureClusterer.TRAINING_SET_PERCENTAGE = args.getDoubleParam(ArgumentsParser.PARAM_MARKOV_WORKLOAD_SPLIT);
            LOG.debug("TRAINING_SET_PERCENTAGE = " + FeatureClusterer.TRAINING_SET_PERCENTAGE);
        }
        if (args.hasDoubleParam(ArgumentsParser.PARAM_MARKOV_PARTITIONS)) {
            FeatureClusterer.PARTITION_EVALUATION_FACTOR = args.getDoubleParam(ArgumentsParser.PARAM_MARKOV_PARTITIONS);
            LOG.debug("PARTITION_EVALUATION_FACTOR = " + FeatureClusterer.PARTITION_EVALUATION_FACTOR);
        }
        if (args.hasDoubleParam(ArgumentsParser.PARAM_MARKOV_TOPK)) {
            FeatureClusterer.ATTRIBUTESET_TOP_K = args.getDoubleParam(ArgumentsParser.PARAM_MARKOV_TOPK);
            LOG.debug("ATTRIBUTESET_TOP_K = " + FeatureClusterer.ATTRIBUTESET_TOP_K);
        }
        if (args.hasIntParam(ArgumentsParser.PARAM_MARKOV_ROUNDS)) {
            FeatureClusterer.NUM_ROUNDS = args.getIntParam(ArgumentsParser.PARAM_MARKOV_ROUNDS);
            LOG.debug("NUM_ROUNDS = " + FeatureClusterer.NUM_ROUNDS);
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
        
        FeatureExtractor fextractor = new FeatureExtractor(args.catalog_db);
        Instances data = fextractor.calculate(args.workload).get(catalog_proc).export(catalog_proc.getName());
        
        assert(args.workload.getTransactionCount() == data.numInstances());
        
        FeatureClusterer fclusterer = null;
        if (args.hasParam(ArgumentsParser.PARAM_WORKLOAD_RANDOM_PARTITIONS)) {
            Histogram h = new PartitionEstimator(args.catalog_db).buildBasePartitionHistogram(args.workload);
//            System.err.println("# OF PARTITIONS: " + h.getValueCount());
//            h.setKeepZeroEntries(true);
//            for (Integer p : CatalogUtil.getAllPartitionIds(args.catalog_db)) {
//                if (h.contains(p) == false) h.put(p, 0);
//            }
//            System.err.println(h);
//            System.exit(1);
//            
            Set<Integer> partitions = h.values();
            fclusterer = new FeatureClusterer(catalog_proc, args.workload, args.param_correlations, partitions);
        } else {
            fclusterer = new FeatureClusterer(catalog_proc, args.workload, args.param_correlations);    
        }
        
        Set<Attribute> attributes = prefix2attributes(data,
            FeatureUtil.getFeatureKeyPrefix(ParamArrayLengthFeature.class, catalog_proc.getParameters().get(4)),
            FeatureUtil.getFeatureKeyPrefix(ParamNumericValuesFeature.class, catalog_proc.getParameters().get(1))
        );
//        System.err.println("Attributes: " + attributes);
        Instances instances[] = fclusterer.splitWorkload(data);
        AttributeSet aset = fclusterer.createAttributeSet(attributes, instances);
        fclusterer.cleanup();
        System.err.println(aset + "\nCost: " + aset.getCost());
    }
    
}
