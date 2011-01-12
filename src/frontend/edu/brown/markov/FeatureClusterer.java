package edu.brown.markov;

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
     * Split the workload into separate workloads based on the txn's base partition
     * @param workload
     * @return
     * @throws Exception
     */
    private Map<Integer, Workload> splitWorkload(Workload workload) throws Exception {
        Map<Integer, Workload> ret = new TreeMap<Integer, Workload>();
        for (TransactionTrace txn_trace : workload.getTransactions()) {
            Procedure catalog_proc = txn_trace.getCatalogItem(this.catalog_db);
            Integer base_partition = this.p_estimator.getBasePartition(catalog_proc, txn_trace.getParams());
            Workload w = ret.get(base_partition);
            if (w == null) {
                w = new Workload(this.catalog_db.getCatalog());
                ret.put(base_partition, w);
            }
            w.addTransaction(catalog_proc, txn_trace);
        } // FOR
        return (ret);
    }
    
    protected void findBestAttributeSet(FeatureSet fset, Instances data, Procedure catalog_proc) throws Exception {
        AttributeSet best_set = null;
        
        int round = 1;
        List<Attribute> all_attributes = (List<Attribute>)CollectionUtil.addAll(new ArrayList<Attribute>(), data.enumerateAttributes());

        // FIXME: Need to split the input data set into separate data sets
        Instances trainingData = new Instances(data);
        int trainingCnt = trainingData.numInstances();
        
        Instances validationData = new Instances(data);
        int validationCnt = trainingData.numInstances();
        
        while (round < 10) {
            final boolean trace = LOG.isTraceEnabled();
            final boolean debug = LOG.isDebugEnabled();
            
            if (debug) {
                if (round == 1) {
                    LOG.debug("# of Training Instances:  " + trainingCnt);
                    LOG.debug("# of Validation Instances:  " + validationCnt);
                }
                
                Map<String, Object> m = new ListOrderedMap<String, Object>();
                m.put("Round #", String.format("%02d", round));
                m.put("Best Set", best_set);
                m.put("Best Cost", (best_set != null ? best_set.getCost() : null));
                LOG.debug("\n" + StringUtil.formatMaps(":", true, true, m));
            }

            // The AttributeSets created in this round
            SortedSet<AttributeSet> attr_sets = new TreeSet<AttributeSet>();
            
            for (Set<Attribute> s : UniqueCombinationIterator.factory(all_attributes, round)) {
                AttributeSet attr_set = new AttributeSet(s);
                
                // Build our clusterer
                AbstractClusterer clusterer = this.createClusterer(trainingData, attr_set, catalog_proc);
                if (trace) LOG.trace(String.format("Constructing Clusterer using %d Attributes: %s", attr_set.size(), attr_set)); 
                
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
                if (trace) LOG.trace("Total Number of Clusters: " + cluster_h.getValueCount() + "\n" + cluster_h);
                
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
                    total_cost += costmodel.estimateTransactionCost(catalog_db, txn_trace);
                } // FOR
                if (trace) LOG.trace(String.format("Total Estimated Cost: %.03d", total_cost));
                attr_set.setCost(total_cost);
                attr_sets.add(attr_set);
            } // WHILE (AttributeSet)
            
            // Now figure out what the top-k AttributeSets from this round
            
            break;
        } // WHILE (round)
        
    }
    
    /**
     * 
     * @param data
     * @param round
     * @throws Exception
     */
    protected AbstractClusterer createClusterer(Instances data, AttributeSet attrset, Procedure catalog_proc) throws Exception {
        LOG.info(String.format("Clustering %d %s instances with %d attributes", data.numInstances(), CatalogUtil.getDisplayName(catalog_proc), data.numAttributes()));
        
        // Create the filter we need so that we only include the attributes in the given AttributeSet
        Filter filter = attrset.createFilter(data);
        
        // Using our training set to build the clusterer
        int num_partitions = CatalogUtil.getNumberOfPartitions(catalog_db);
        SimpleKMeans kmeans_clusterer = new SimpleKMeans();
        kmeans_clusterer.setNumClusters(num_partitions);
        kmeans_clusterer.setSeed(this.rand.nextInt());
        
        FilteredClusterer filtered_clusterer = new FilteredClusterer();
        filtered_clusterer.setFilter(filter);
        filtered_clusterer.setClusterer(kmeans_clusterer);
        
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
    
}
