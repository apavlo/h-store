package edu.brown.markov;

import java.io.File;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.junit.Test;
import org.voltdb.VoltProcedure;
import org.voltdb.benchmark.tpcc.procedures.neworder;
import org.voltdb.catalog.Procedure;

import weka.classifiers.Classifier;
import weka.clusterers.AbstractClusterer;
import weka.core.Attribute;
import weka.core.Instance;
import weka.core.Instances;
import weka.filters.Filter;
import weka.filters.unsupervised.attribute.NumericToNominal;
import edu.brown.BaseTestCase;
import edu.brown.catalog.CatalogUtil;
import edu.brown.markov.FeatureClusterer.SplitType;
import edu.brown.markov.features.BasePartitionFeature;
import edu.brown.markov.features.FeatureUtil;
import edu.brown.markov.features.ParamArrayLengthFeature;
import edu.brown.markov.features.ParamHashPartitionFeature;
import edu.brown.statistics.Histogram;
import edu.brown.statistics.ObjectHistogram;
import edu.brown.utils.CollectionUtil;
import edu.brown.utils.ProjectType;
import edu.brown.utils.StringUtil;
import edu.brown.workload.TransactionTrace;
import edu.brown.workload.Workload;
import edu.brown.workload.filters.ProcedureLimitFilter;
import edu.brown.workload.filters.ProcedureNameFilter;
import edu.brown.hstore.conf.HStoreConf;

/**
 * NOTE: 2012-10-20
 * I am getting random JVM crashes with some of these test cases.
 * I think it's because of Weka, but I don't have time to look into it
 * I've commented out the tests for now.
 * @author pavlo
 */
public class TestFeatureClusterer extends BaseTestCase {

    private static final Class<? extends VoltProcedure> TARGET_PROCEDURE = neworder.class;
    private static final int WORKLOAD_XACT_LIMIT = 1000;
//    private static final int BASE_PARTITION = 1;
    private static final int NUM_PARTITIONS = 50;

    private static Procedure catalog_proc;
    private static Workload workload;
    private static Instances data;
    
    private FeatureClusterer fclusterer;
    
    @Override
    protected void setUp() throws Exception {
        super.setUp(ProjectType.TPCC);
        this.addPartitions(NUM_PARTITIONS);
        
        HStoreConf.singleton().site.markov_path_caching = false;
        
        if (workload == null) {
            catalog_proc = this.getProcedure(TARGET_PROCEDURE);
            
            File file = this.getWorkloadFile(ProjectType.TPCC);
            workload = new Workload(catalog);

            // Check out this beauty:
            // (1) Filter by procedure name
            // (2) Filter on partitions that start on our BASE_PARTITION
            // (3) Filter to only include multi-partition txns
            // (4) Another limit to stop after allowing ### txns
            // Where is your god now???
            edu.brown.workload.filters.Filter filter = new ProcedureNameFilter(false)
                    .include(TARGET_PROCEDURE.getSimpleName())
//                    .attach(new ProcParameterValueFilter().include(1, new Long(5))) // D_ID
//                    .attach(new ProcParameterArraySizeFilter(CatalogUtil.getArrayProcParameters(catalog_proc).get(0), 10, ExpressionType.COMPARE_EQUAL))
//                    .attach(new BasePartitionTxnFilter(p_estimator, BASE_PARTITION))
//                    .attach(new MultiPartitionTxnFilter(p_estimator))
                    .attach(new ProcedureLimitFilter(WORKLOAD_XACT_LIMIT));
            workload.load(file, catalog_db, filter);
            assert(workload.getTransactionCount() > 0);
            
            // Now extract the FeatureSet that we will use in our tests
            Map<Procedure, FeatureSet> fsets = new FeatureExtractor(catalogContext, p_estimator).calculate(workload);
            FeatureSet fset = fsets.get(catalog_proc);
            assertNotNull(fset);
            data = fset.export(catalog_proc.getName(), false);
            
            NumericToNominal weka_filter = new NumericToNominal();
            weka_filter.setInputFormat(data);
            data = Filter.useFilter(data, weka_filter);
        }
        assertNotNull(data);
        
        fclusterer = new FeatureClusterer(catalogContext, catalog_proc, workload, catalogContext.getAllPartitionIds());
    }
    
    
    /**
     * testSplitPercentages
     */
    @Test
    public void testSplitPercentages() {
        double total = 0.0d;
        for (SplitType stype : FeatureClusterer.SplitType.values()) {
            total += stype.getPercentage();
        }
        assertEquals(1.0d, total);
    }
    
//    /**
//     * testCalculateGlobalCost
//     */
//    @Test
//    public void testCalculateGlobalCost() throws Exception {
//        this.fclusterer.splitWorkload(data);
//        this.fclusterer.calculateGlobalCost();
//        int counters[] = this.fclusterer.getGlobalCounters();
//        assertNotNull(counters);
//        for (int i = 0; i < counters.length; i++) {
//            int val = counters[i];
//            assert(val >= 0) : String.format("Invalid Counter[%d] => %d", i, val);
//        } // FOR
//    }
    
//    /**
//     * testCalculate
//     */
//    @Test
//    public void testCalculate() throws Exception {
//        this.fclusterer.setNumRounds(1);
//        this.fclusterer.setAttributeTopK(0.50);
//        MarkovAttributeSet aset = this.fclusterer.calculate(data);
//        assertNotNull(aset);
//        
//        System.err.println(aset);
//        System.err.println("COST: " + aset.getCost());
//        
//        
//    }
    
    /**
     * testCreateMarkovAttributeSetFilter
     */
    @Test
    public void testCreateMarkovAttributeSetFilter() throws Exception {
        // Test that we can create a filter from an MarkovAttributeSet
        MarkovAttributeSet aset = new MarkovAttributeSet(data, FeatureUtil.getFeatureKeyPrefix(ParamArrayLengthFeature.class));
        assertEquals(CatalogUtil.getArrayProcParameters(catalog_proc).size(), aset.size());
        
        Filter filter = aset.createFilter(data);
        Instances newData = Filter.useFilter(data, filter);
        for (int i = 0, cnt = newData.numInstances(); i < cnt; i++) {
            Instance processed = newData.instance(i);
//            System.err.println(processed);
            assertEquals(aset.size(), processed.numAttributes());
        } // WHILE
        assertEquals(data.numInstances(), newData.numInstances());
//        System.err.println("MarkovAttributeSet: " + aset);
        
    }
    
    /**
     * testCreateClusterer
     */
    @Test
    public void testCreateClusterer() throws Exception {
        // Construct a simple MarkovAttributeSet that only contains the BasePartitionFeature
        MarkovAttributeSet base_aset = new MarkovAttributeSet(data, FeatureUtil.getFeatureKeyPrefix(BasePartitionFeature.class));
        assertFalse(base_aset.isEmpty());
        int base_partition_idx = CollectionUtil.first(base_aset).index();
        
        AbstractClusterer clusterer = this.fclusterer.createClusterer(base_aset, data);
        assertNotNull(clusterer);
        
        // Make sure that each Txn gets mapped to the same cluster as its base partition
        Map<Integer, Histogram<Integer>> p_c_xref = new HashMap<Integer, Histogram<Integer>>();
        for (int i = 0, cnt = data.numInstances(); i < cnt; i++) {
            Instance inst = data.instance(i);
            assertNotNull(inst);
            long txn_id = FeatureUtil.getTransactionId(inst);

            TransactionTrace txn_trace = workload.getTransaction(txn_id);
            assertNotNull(txn_trace);
            Integer base_partition = p_estimator.getBasePartition(txn_trace);
            assertNotNull(base_partition);
            assertEquals(base_partition.intValue(), (int)inst.value(base_partition_idx));

            int c = clusterer.clusterInstance(inst);
            Histogram<Integer> h = p_c_xref.get(base_partition);
            if (h == null) {
                h = new ObjectHistogram<Integer>();
                p_c_xref.put(base_partition, h);
            }
            h.put(c);
        } // FOR
        
//        System.err.println(StringUtil.formatMaps(p_c_xref));
//        Set<Integer> c_p_xref = new HashSet<Integer>();
//        for (Entry<Integer, Histogram> e : p_c_xref.entrySet()) {
//            Set<Integer> clusters = e.getValue().values();
//            
//            // Make sure that each base partition is only mapped to one cluster
//            assertEquals(e.getKey().toString(), 1, clusters.size());
//            
//            // Make sure that two different base partitions are not mapped to the same cluster
//            assertFalse(c_p_xref.contains(CollectionUtil.getFirst(clusters)));
//            c_p_xref.addAll(clusters);
//        } // FOR
    }

//    /**
//     * testCalculateAttributeSetCost
//     */
//    @Test
//    public void testCalculateAttributeSetCost() throws Exception {
//        Set<Attribute> attributes = FeatureClusterer.prefix2attributes(data,
//            FeatureUtil.getFeatureKeyPrefix(ParamArrayLengthFeature.class, this.getProcParameter(catalog_proc, 4)),
//            FeatureUtil.getFeatureKeyPrefix(ParamHashPartitionFeature.class, this.getProcParameter(catalog_proc, 1))
//        );
//        
//        Instances instances[] = fclusterer.splitWorkload(data);
//        assertNotNull(instances);
//        MarkovAttributeSet aset = new MarkovAttributeSet(attributes);
//        assertNotNull(aset);
//        fclusterer.calculateAttributeSetCost(aset);
//        assert(aset.getCost() > 0);
//    }
    
//    /**
//     * testGenerateDecisionTree
//     */
//    @Test
//    public void testGenerateDecisionTree() throws Exception {
//        Set<Attribute> attributes = FeatureClusterer.prefix2attributes(data,
//              FeatureUtil.getFeatureKeyPrefix(ParamArrayLengthFeature.class, this.getProcParameter(catalog_proc, 4)),
//              FeatureUtil.getFeatureKeyPrefix(ParamHashPartitionFeature.class, this.getProcParameter(catalog_proc, 1))
//        );
//        MarkovAttributeSet aset = new MarkovAttributeSet(attributes);
//        assertNotNull(aset);
//
//        Histogram<String> key_h = new Histogram<String>();
//        int key_len = aset.size();
//        for (int i = 0, cnt = data.numInstances(); i < cnt; i++) {
//            Instance inst = data.instance(i);
//            Object key[] = new Object[key_len];
//            for (int ii = 0; ii < key_len; ii++) {
//                key[ii] = inst.value(aset.get(ii));
//            }
//            key_h.put(Arrays.toString(key));
//        } // FOR
//        System.err.println("Number of Elements: " + key_h.getValueCount());
//        System.err.println(key_h);
//        System.err.println(StringUtil.repeat("+", 100));
//        
////        Instances instances[] = fclusterer.splitWorkload(data);
////        assertNotNull(instances);
//        
//        AbstractClusterer clusterer = fclusterer.createClusterer(aset, data);
//        assertNotNull(clusterer);
//        
//        Classifier classifier = fclusterer.generateDecisionTree(clusterer, aset, data);
//        assertNotNull(classifier);
//    }

}