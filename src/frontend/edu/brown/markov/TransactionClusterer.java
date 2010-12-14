package edu.brown.markov;

import java.util.*;

import org.apache.log4j.Logger;
import org.voltdb.catalog.Database;
import org.voltdb.catalog.Procedure;

import weka.clusterers.EM;
import weka.core.Attribute;
import weka.core.Instances;
import weka.filters.Filter;
import weka.filters.unsupervised.instance.RemoveWithValues;

import edu.brown.catalog.CatalogUtil;
import edu.brown.markov.features.AbstractFeature;
import edu.brown.markov.features.BasePartitionFeature;
import edu.brown.utils.PartitionEstimator;
import edu.brown.workload.TransactionTrace;
import edu.brown.workload.Workload;

public class TransactionClusterer {
    private static final Logger LOG = Logger.getLogger(TransactionClusterer.class);

    private final Database catalog_db;
    private final PartitionEstimator p_estimator;
    private final Random rand = new Random();
       
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
    
    protected void cluster(Instances data) throws Exception {
        Set<Attribute> best_attribute_set = new TreeSet<Attribute>();
        
        
        EM clusterer = new EM();
        clusterer.setSeed(this.rand.nextInt());
        clusterer.buildClusterer(data);
    }
    
    public void calculate(Procedure catalog_proc, FeatureSet fset) throws Exception {
        Instances data = fset.export(catalog_proc.getName());
        List<Integer> all_partitions = CatalogUtil.getAllPartitionIds(this.catalog_db);
        
        String prefix_key = AbstractFeature.getFeatureKeyPrefix(BasePartitionFeature.class);
        Attribute base_partition_attr = data.attribute(prefix_key);
        
        
        for (Integer base_partition : all_partitions) {
            
            // Include all values < (base_partition+1)
            RemoveWithValues filter0 = new RemoveWithValues();
            filter0.setAttributeIndex(Integer.toString(base_partition_attr.index()));
            filter0.setSplitPoint(base_partition + 1.0);
            
            // Include all values >= base_partition
            RemoveWithValues filter1 = new RemoveWithValues();
            filter1.setAttributeIndex(filter0.getAttributeIndex());
            filter1.setSplitPoint(base_partition);
            filter1.setInvertSelection(true);
            
            Instances filtered_data = Filter.useFilter(Filter.useFilter(data, filter0), filter1);
            if (filtered_data.numInstances() == 0) {
                LOG.warn("No instances found for " + catalog_proc + " at base partition #" + base_partition);
                continue;
            }
        } // FOR
            
//            
//            
//            Workload proc_workload = new Workload(workload, new ProcedureNameFilter().include(catalog_proc.getName()));
//            Map<Integer, Workload> base_workloads = this.splitWorkload(proc_workload);
//            Map<Integer, FeatureSet> p_fsets = new TreeMap<Integer, FeatureSet>();
//            
//            for (Entry<Integer, Workload> e : base_workloads.entrySet()) {
//                TransactionFeatureExtractor extractor = new TransactionFeatureExtractor(this.catalog_db, this.p_estimator);
//                FeatureSet fset = extractor.calculate(e.getValue()).get(catalog_proc);
//                assert(fset != null) : "Failed to calculate " + catalog_proc + " FeatureSet for partition #" + e.getKey(); 
//                p_fsets.put(e.getKey(), fset);
//            } // FOR
//            
//        }
        
        
    }
    
}
