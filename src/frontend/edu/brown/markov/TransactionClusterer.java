package edu.brown.markov;

import java.util.*;

import org.apache.commons.collections15.set.ListOrderedSet;
import org.apache.log4j.Logger;
import org.voltdb.catalog.Database;
import org.voltdb.catalog.Procedure;

import weka.clusterers.EM;
import weka.clusterers.SimpleKMeans;
import weka.core.Attribute;
import weka.core.Instance;
import weka.core.Instances;
import weka.filters.Filter;
import weka.filters.unsupervised.attribute.Remove;
import weka.filters.unsupervised.instance.RemoveWithValues;

import edu.brown.catalog.CatalogUtil;
import edu.brown.markov.features.BasePartitionFeature;
import edu.brown.markov.features.FeatureUtil;
import edu.brown.statistics.Histogram;
import edu.brown.utils.CollectionUtil;
import edu.brown.utils.PartitionEstimator;
import edu.brown.utils.StringUtil;
import edu.brown.workload.TransactionTrace;
import edu.brown.workload.Workload;

public class TransactionClusterer {
    private static final Logger LOG = Logger.getLogger(TransactionClusterer.class);

    private final Database catalog_db;
    private final PartitionEstimator p_estimator;
    private final Random rand = new Random();
       
    /**
     * A set of attributes and their cost
     */
    private class AttributeSet extends ListOrderedSet<Attribute> implements Comparable<AttributeSet> {
        private static final long serialVersionUID = 1L;
        private Double cost;

        public Instances copyData(Instances data) throws Exception {
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
            for (int i = 0, cnt = data.numInstances(); i < cnt; i++) {
                filter.input(data.instance(i));
            } // FOR
            filter.batchFinished();
            
            Instances newData = filter.getOutputFormat();
            Instance processed;
            while ((processed = filter.output()) != null) {
                newData.add(processed);
            } // WHILE
            return (newData);
        }
        
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
    public TransactionClusterer(Database catalog_db) {
        this.catalog_db = catalog_db;
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
    
    protected void findBestAttributeSet(Instances data) throws Exception {
        SortedSet<AttributeSet> attr_sets = new TreeSet<AttributeSet>();
        int round = 0;

        // Create the initial set of single-element AttributeSets and calculate
        // how well the Markov models perform when using them
        List<Attribute> attributes = new ArrayList<Attribute>();
        Enumeration e = data.enumerateAttributes();
        while (e.hasMoreElements()) {
            Attribute attr = (Attribute)e.nextElement();
            attributes.add(attr);
            
            AttributeSet attr_set = new AttributeSet();
            attr_set.add(attr);
            
            Instances new_data = attr_set.copyData(data);
            this.doCluster(new_data, round);
            System.exit(1);
            
        } // WHILE
    }
    
    protected void generateMarkovModels(AttributeSet attr_set) throws Exception {
        
        
    }
    
    /**
     * 
     * @param data
     * @param round
     * @throws Exception
     */
    private void doCluster(Instances data, int round) throws Exception {
        LOG.info(String.format("Clustering %d instances with %d attributes", data.numInstances(), data.numAttributes()));
        
        int num_partitions = CatalogUtil.getNumberOfPartitions(catalog_db);
        SimpleKMeans clusterer = new SimpleKMeans();
        clusterer.setNumClusters(num_partitions);
        clusterer.setSeed(this.rand.nextInt());
        clusterer.buildClusterer(data);
        
        
        Instances copy = new Instances(data);
        Histogram h = new Histogram();
        for (int i = 0, cnt = copy.numInstances(); i < cnt; i++) {
            Instance inst = copy.instance(i);
            int c = (int)clusterer.clusterInstance(inst);
            h.put(c);
            // System.err.println(String.format("[%02d] %s => %d", i, inst, c));
        } // FOR
        LOG.info("Total Number of Clusters: " + h.getValueCount() + "\n" + h);
    }

    
    public void calculate(Procedure catalog_proc, FeatureSet fset) throws Exception {
        Instances data = fset.export(catalog_proc.getName());
        List<Integer> all_partitions = CatalogUtil.getAllPartitionIds(this.catalog_db);
        
        this.findBestAttributeSet(data);
        
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
