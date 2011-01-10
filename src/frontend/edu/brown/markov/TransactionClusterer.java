package edu.brown.markov;

import java.util.*;
import java.util.Map.Entry;

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

public class TransactionClusterer {
    private static final Logger LOG = Logger.getLogger(TransactionClusterer.class);

    // What percentage of the input data should be used for training versus calculating the cost?
    private static final double TRAINING_SET_PERCENTAGE = 0.60;
    
    private final Database catalog_db;
    private final Workload workload;
    private final ParameterCorrelations correlations;
    private final PartitionEstimator p_estimator;
    private final Random rand = new Random();
       
    /**
     * A set of attributes and their cost
     */
    private class AttributeSet extends ListOrderedSet<Attribute> implements Comparable<AttributeSet> {
        private static final long serialVersionUID = 1L;
        private Double cost;

        public AttributeSet(Set<Attribute> items) {
            super(items);
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
            filter.setAttributeIndices(StringUtil.join(",", to_remove));
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
    public TransactionClusterer(Database catalog_db, Workload workload, ParameterCorrelations correlations) {
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
        SortedSet<AttributeSet> attr_sets = new TreeSet<AttributeSet>();
        int round = 1;
        List<Attribute> all_attributes = (List<Attribute>)CollectionUtil.addAll(new ArrayList<Attribute>(), data.enumerateAttributes());

        // FIXME: Need to split the input data set into separate data sets
        Instances trainingData = new Instances(data);
        Instances validationData = new Instances(data);
        
        while (round < 10) {
            for (Set<Attribute> s : UniqueCombinationIterator.factory(all_attributes, round)) {
                AttributeSet attr_set = new AttributeSet(s);
                
                // Build our clusterer
                AbstractClusterer clusterer = this.doCluster(trainingData, attr_set, catalog_proc);
                
                // Now iterate over validation set and construct Markov models
                // We have to know which field is our txn_id so that we can quickly access it
                MarkovGraphsContainer markovs = new MarkovGraphsContainer();
                Map<Integer, Integer> cluster_partition_xref = new HashMap<Integer, Integer>();
                Histogram h = new Histogram();
                for (int i = 0, cnt = trainingData.numInstances(); i < cnt; i++) {
                    // Grab the Instance and throw it at the the clusterer to get the target cluster
                    // The original data set is going to have the txn id that we need to grab 
                    // the proper TransactionTrace record from the workload
                    Instance inst = trainingData.instance(i);
                    int c = (int)clusterer.clusterInstance(inst);
                    h.put(c);
                    
                    long txn_id = (long)inst.value(TransactionFeatureExtractor.TXNID_ATTRIBUTE_IDX);
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
                LOG.info("Total Number of Clusters: " + h.getValueCount() + "\n" + h);
                
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
                Map<Long, Integer> txnid_cluster_xref = new HashMap<Long, Integer>();
                for (int i = 0, cnt = validationData.numInstances(); i < cnt; i++) {
                    Instance inst = validationData.instance(i);
                    long txn_id = (long)inst.value(TransactionFeatureExtractor.TXNID_ATTRIBUTE_IDX);
                    int c = (int)clusterer.clusterInstance(inst);
                    txnid_cluster_xref.put(txn_id, c);
                } // FOR
                
                
                
            } // WHILE (AttributeSet)
        } // WHILE (round)
        
//        // Create the initial set of single-element AttributeSets and calculate
//        // how well the Markov models perform when using them
//        List<Attribute> attributes = new ArrayList<Attribute>();
//        Enumeration e = data.enumerateAttributes();
//        while (e.hasMoreElements()) {
//            Attribute attr = (Attribute)e.nextElement();
//            attributes.add(attr);
//            
//            
//            attr_set.add(attr);
//            
//            System.exit(1);
//            
//        } // WHILE
    }
    
    private void estimateCost(Map<Integer, TransactionEstimator> t_estimators, Instances data, AbstractClusterer clusterer) throws Exception {
        for (int i = 0, cnt = data.numInstances(); i < cnt; i++) {
            Instance inst = data.instance(i);
            int c = (int)clusterer.clusterInstance(inst);
            TransactionEstimator t_estimator = t_estimators.get(c);
            assert(t_estimator != null) : "Cluster #" + c;
            
            long txn_id = (long)inst.value(TransactionFeatureExtractor.TXNID_ATTRIBUTE_IDX);
            TransactionTrace txn_trace = this.workload.getTransaction(txn_id);
            assert(txn_trace != null) : "Invalid TxnId #" + txn_id + "\n" + inst;
            
            Estimate est = t_estimator.startTransaction(txn_trace.getTransactionId(), txn_trace.getCatalogItem(this.catalog_db), txn_trace.getParams());
            assert(est != null);
            
        }

        
    }
    
    /**
     * 
     * @param data
     * @param round
     * @throws Exception
     */
    private AbstractClusterer doCluster(Instances data, AttributeSet attrset, Procedure catalog_proc) throws Exception {
        LOG.info(String.format("Clustering %d %s instances with %d attributes", data.numInstances(), CatalogUtil.getDisplayName(catalog_proc), data.numAttributes()));
        
        // Create the filter we need so that we only include the attributes in the given AttributeSet
        Filter filter = attrset.createFilter(data);
        
        // Using our training set to build the clusterer
        // FIXME: Need to split input into different sets
        int num_partitions = CatalogUtil.getNumberOfPartitions(catalog_db);
        SimpleKMeans kmeans_clusterer = new SimpleKMeans();
        kmeans_clusterer.setNumClusters(num_partitions);
        kmeans_clusterer.setSeed(this.rand.nextInt());
        
        FilteredClusterer clusterer = new FilteredClusterer();
        clusterer.setFilter(filter);
        clusterer.setClusterer(kmeans_clusterer);
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
