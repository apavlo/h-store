package edu.brown.markov;

import java.io.File;
import java.util.*;
import java.util.Map.Entry;

import org.apache.commons.collections15.map.ListOrderedMap;
import org.apache.commons.collections15.set.ListOrderedSet;
import org.apache.log4j.Logger;
import org.voltdb.catalog.Database;
import org.voltdb.catalog.Procedure;
import org.voltdb.utils.Pair;

import weka.clusterers.AbstractClusterer;
import weka.clusterers.Clusterer;
import weka.clusterers.EM;
import weka.clusterers.FilteredClusterer;
import weka.clusterers.SimpleKMeans;
import weka.core.Attribute;
import weka.core.Instance;
import weka.core.Instances;
import weka.filters.Filter;
import weka.filters.unsupervised.attribute.Remove;
import weka.filters.unsupervised.instance.RemoveWithValues;

import edu.brown.catalog.CatalogUtil;
import edu.brown.correlations.ParameterCorrelations;
import edu.brown.costmodel.MarkovCostModel;
import edu.brown.markov.TransactionEstimator.Estimate;
import edu.brown.markov.features.BasePartitionFeature;
import edu.brown.markov.features.FeatureUtil;
import edu.brown.markov.features.TransactionIdFeature;
import edu.brown.statistics.Histogram;
import edu.brown.utils.ArgumentsParser;
import edu.brown.utils.CollectionUtil;
import edu.brown.utils.PartitionEstimator;
import edu.brown.utils.StringUtil;
import edu.brown.utils.UniqueCombinationIterator;
import edu.brown.workload.TransactionTrace;
import edu.brown.workload.Workload;

public class FeatureClusterer {
    private static final Logger LOG = Logger.getLogger(FeatureClusterer.class);

    // What percentage of the input data should be used for training versus calculating the cost?
    private static final double TRAINING_SET_PERCENTAGE = 0.60;
    
    // For each search round, we will only propagate the attributes found this these top-k AttibuteSets 
    private static final double ATTRIBUTESET_TOP_K = 0.25;
    
    private final Database catalog_db;
    private final Workload workload;
    private final ParameterCorrelations correlations;
    private final PartitionEstimator p_estimator;
    private final Random rand = new Random(0);
       
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
    
    /**
     * Constructor
     * @param catalog_db
     */
    public FeatureClusterer(Database catalog_db, Workload workload, ParameterCorrelations correlations) {
        this.catalog_db = catalog_db;
        this.workload = workload;
        this.correlations = correlations;
        this.p_estimator = new PartitionEstimator(catalog_db);
    }
    
    /**
     * 
     * @param data
     * @return
     */
    protected Pair<Instances, Instances> splitWorkload(Instances data) {
        int all_cnt = data.numInstances();
        int training_cnt = (int)Math.round(all_cnt * TRAINING_SET_PERCENTAGE);
        Instances trainingData = new Instances(data, 0, training_cnt);
        Instances validationData = new Instances(data, training_cnt, all_cnt - training_cnt);
        return (Pair.of(trainingData, validationData));
    }

    /**
     * 
     * @param fset
     * @param data
     * @param catalog_proc
     * @throws Exception
     */
    protected void findBestAttributeSet(FeatureSet fset, Instances data, Procedure catalog_proc) throws Exception {
        AttributeSet best_set = null;
        
        // Get the list of all the attributes that we are going to want to try to cluster on
        // We want to always remove the first attribute because that's the TransactionId
        ListOrderedSet<Attribute> all_attributes = (ListOrderedSet<Attribute>)CollectionUtil.addAll(new ListOrderedSet<Attribute>(), data.enumerateAttributes());
        all_attributes.remove(0);

        // Split the input data set into separate data sets
        Pair<Instances, Instances> p = this.splitWorkload(data);
        Instances trainingData = p.getFirst();
        int trainingCnt = trainingData.numInstances();
        
        Instances validationData = p.getSecond();
        int validationCnt = validationData.numInstances();
        
        // List of all AttributeSets ever created
        SortedSet<AttributeSet> all_asets = new TreeSet<AttributeSet>();

        int round = 0;
        while (round++ < 10) {
            final boolean trace = LOG.isTraceEnabled();
            final boolean debug = LOG.isDebugEnabled();
            
            if (debug) {
                if (round == 1) {
                    LOG.debug("# of Training Instances:    " + trainingCnt);
                    LOG.debug("# of Validation Instances:  " + validationCnt);
                }
                
                Map<String, Object> m = new ListOrderedMap<String, Object>();
                m.put("Round #", String.format("%02d", round));
                m.put("Best Set", best_set);
                m.put("Best Cost", (best_set != null ? best_set.getCost() : null));
                LOG.debug("\n" + StringUtil.formatMaps(":", true, true, m));
            }

            // The AttributeSets created in this round
            SortedSet<AttributeSet> round_asets = new TreeSet<AttributeSet>();
            
            int aset_ctr = 0;
            for (final Set<Attribute> s : UniqueCombinationIterator.factory(all_attributes, round)) {
                AttributeSet aset = this.createAttributeSet(catalog_proc, s, trainingData, validationData);
                round_asets.add(aset);
                
                if (debug) LOG.debug(String.format("[%03d] Attributes%s => %.03f", aset_ctr++, aset.toString(), aset.getCost()));
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
            if (round == 1) all_attributes.add(data.attribute(1));
            
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
    protected AttributeSet createAttributeSet(Procedure catalog_proc, Set<Attribute> attributes, Instances trainingData, Instances validationData) throws Exception {
        final boolean trace = LOG.isTraceEnabled();
        final boolean debug = LOG.isDebugEnabled();
        
        final AttributeSet aset = new AttributeSet(attributes);
        final int trainingCnt = trainingData.numInstances();
        final int validationCnt = validationData.numInstances();
        
        // Build our clusterer
        if (trace) LOG.trace(String.format("Constructing Clusterer using %d Attributes: %s", aset.size(), aset));
        AbstractClusterer clusterer = this.createClusterer(trainingData, aset, catalog_proc);
        
        // Now iterate over validation set and construct Markov models
        // We have to know which field is our txn_id so that we can quickly access it
        MarkovGraphsContainer markovs = new MarkovGraphsContainer();
        Map<Integer, Integer> cluster_partition_xref = new HashMap<Integer, Integer>();
        Histogram cluster_h = new Histogram();
        for (int i = 0; i < trainingCnt; i++) {
            // Grab the Instance and throw it at the the clusterer to get the target cluster
            // The original data set is going to have the txn id that we need to grab 
            // the proper TransactionTrace record from the workload
            Instance inst = trainingData.instance(i);
            int c = (int)clusterer.clusterInstance(inst);
            cluster_h.put(c);
            
            long txn_id = (long)inst.value(FeatureExtractor.TXNID_ATTRIBUTE_IDX);
            TransactionTrace txn_trace = this.workload.getTransaction(txn_id);
            assert(txn_trace != null) : "Invalid TxnId #" + txn_id + "\n" + inst;
            
            // Build up the MarkovGraph for this cluster
            MarkovGraph markov = markovs.get(c, catalog_proc);
            if (markov == null) {
                // XXX: Assume for now that all the instances in the same cluster are at the same base partition
                Integer base_partition = this.p_estimator.getBasePartition(txn_trace);
                assert(base_partition != null);
                markov = new MarkovGraph(catalog_proc, base_partition);
                markovs.put(c, markov.initialize());
                cluster_partition_xref.put(c, base_partition);
            }
            markov.processTransaction(txn_trace, this.p_estimator);
        } // FOR
        if (debug) LOG.debug("Total Number of Clusters: " + cluster_h.getValueCount() + "\n" + cluster_h);
        
        // Now use the validation data set to figure out how well we are able to predict transaction
        // execution paths using the trained Markov graphs
        // We first need to construct a new costmodel and populate it with TransactionEstimators
        MarkovCostModel costmodel = new MarkovCostModel(this.catalog_db, this.p_estimator);
        costmodel.setTransactionClusterMapping(true);
        for (Entry<Integer, Map<Procedure, MarkovGraph>> e : markovs.entrySet()) {
            int base_partition = cluster_partition_xref.get(e.getKey());
            TransactionEstimator t_estimator = new TransactionEstimator(base_partition, this.p_estimator, this.correlations);
            t_estimator.addMarkovGraphs(e.getValue());
            costmodel.addTransactionEstimator(e.getKey(), t_estimator);
        } // FOR
        
        // DEBUG
        MarkovUtil.save(markovs, "/tmp/" + catalog_proc.getName() + ".markovs");
        
        // Now we need a mapping from TransactionIds -> ClusterIds
        // And then calculate the cost of using our cluster configuration to predict txn paths
        double total_cost = 0.0d;
        for (int i = 0; i < validationCnt; i++) {
            Instance inst = validationData.instance(i);
            long txn_id = (long)inst.value(FeatureExtractor.TXNID_ATTRIBUTE_IDX);
            int c = (int)clusterer.clusterInstance(inst);
            costmodel.addTransactionClusterXref(txn_id, c);
            
            // Check that this is a cluster that we've seen before
            if (cluster_h.contains(c) == false) {
                if (debug) LOG.warn(String.format("Txn #%d was mapped to never before seen Cluster #%d", txn_id, c));
            }
            
            TransactionTrace txn_trace = workload.getTransaction(txn_id);
            assert(txn_trace != null);
            double cost = costmodel.estimateTransactionCost(catalog_db, txn_trace);
            if (cost != 0.0d) {
                MarkovGraph markov = markovs.get(c, catalog_proc);
                String actual = MarkovUtil.exportGraphviz(markov, false, markov.getPath(costmodel.getLastActualPath())).writeToTempFile(catalog_proc, 1);
                String estimated = MarkovUtil.exportGraphviz(markov, false, markov.getPath(costmodel.getLastEstimatedPath())).writeToTempFile(catalog_proc, 2);
                
                System.err.println("ACTUAL FILE:    " + actual);
                System.err.println("ESTIMATED FILE: " + estimated);
                
                System.exit(1);
            }
            total_cost += cost;
        } // FOR
        if (trace) LOG.trace(String.format("Total Estimated Cost: %.03f", total_cost));
        aset.setCost(total_cost);
        return (aset);
    }
    
    /**
     * 
     * @param data
     * @param round
     * @throws Exception
     */
    protected AbstractClusterer createClusterer(Instances data, AttributeSet aset, Procedure catalog_proc) throws Exception {
        final boolean trace = LOG.isTraceEnabled();
        if (trace) LOG.trace(String.format("Clustering %d %s instances with %d attributes", data.numInstances(), CatalogUtil.getDisplayName(catalog_proc), aset.size()));
        
        // Create the filter we need so that we only include the attributes in the given AttributeSet
        Filter filter = aset.createFilter(data);
        
        // Using our training set to build the clusterer
        int num_partitions = CatalogUtil.getNumberOfPartitions(catalog_db);
        int seed = 1981; // this.rand.nextInt(); 
        SimpleKMeans inner_clusterer = new SimpleKMeans();
//        EM inner_clusterer = new EM();
        String options[] = {
            "-N", Integer.toString(num_partitions),
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

    
    public void calculate(FeatureSet fset, Procedure catalog_proc) throws Exception {
        Instances data = fset.export(catalog_proc.getName());
        List<Integer> all_partitions = CatalogUtil.getAllPartitionIds(this.catalog_db);
        
        this.findBestAttributeSet(fset, data, catalog_proc);
        
//        String prefix_key = FeatureUtil.getFeatureKeyPrefix(BasePartitionFeature.class);
//        Attribute base_partition_attr = data.attribute(prefix_key);
//        
//        
//        for (Integer base_partition : all_partitions) {
//            
//            // Include all values < (base_partition+1)
//            RemoveWithValues filter0 = new RemoveWithValues();
//            filter0.setAttributeIndex(Integer.toString(base_partition_attr.index()));
//            filter0.setSplitPoint(base_partition + 1.0);
//            
//            // Include all values >= base_partition
//            RemoveWithValues filter1 = new RemoveWithValues();
//            filter1.setAttributeIndex(filter0.getAttributeIndex());
//            filter1.setSplitPoint(base_partition);
//            filter1.setInvertSelection(true);
//            
//            Instances filtered_data = Filter.useFilter(Filter.useFilter(data, filter0), filter1);
//            if (filtered_data.numInstances() == 0) {
//                LOG.warn("No instances found for " + catalog_proc + " at base partition #" + base_partition);
//                continue;
//            }
//        } // FOR
        
    }

    public static void main(String[] vargs) throws Exception {
        ArgumentsParser args = ArgumentsParser.load(vargs);
        args.require(
            ArgumentsParser.PARAM_CATALOG,
            ArgumentsParser.PARAM_WORKLOAD,
            ArgumentsParser.PARAM_CORRELATIONS
        );
        
        String proc_name = args.getOptParam(0);
        Procedure catalog_proc = args.catalog_db.getProcedures().getIgnoreCase(proc_name);
        assert(catalog_proc != null) : proc_name;
        
        File fset_path = new File(args.getOptParam(1));
        assert(fset_path.exists()) : fset_path.getAbsolutePath();
        
        FeatureSet fset = new FeatureSet();
        fset.load(fset_path.getAbsolutePath(), args.catalog_db);
        
        FeatureClusterer fclusterer = new FeatureClusterer(args.catalog_db, args.workload, args.param_correlations);
        fclusterer.calculate(fset, catalog_proc);
    }
    
}
