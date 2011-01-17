package edu.brown.markov;

import java.io.File;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;

import org.apache.commons.collections15.set.ListOrderedSet;
import org.junit.Test;
import org.voltdb.VoltProcedure;
import org.voltdb.benchmark.tpcc.procedures.neworder;
import org.voltdb.catalog.Procedure;
import org.voltdb.utils.Pair;

import weka.clusterers.AbstractClusterer;
import weka.core.Attribute;
import weka.core.Instance;
import weka.core.Instances;
import weka.filters.Filter;
import edu.brown.BaseTestCase;
import edu.brown.catalog.CatalogUtil;
import edu.brown.correlations.ParameterCorrelations;
import edu.brown.markov.FeatureClusterer.AttributeSet;
import edu.brown.markov.features.BasePartitionFeature;
import edu.brown.markov.features.FeatureUtil;
import edu.brown.markov.features.ParamArrayLengthFeature;
import edu.brown.markov.features.ParamNumericValuesFeature;
import edu.brown.markov.features.TransactionIdFeature;
import edu.brown.statistics.Histogram;
import edu.brown.utils.CollectionUtil;
import edu.brown.utils.ProjectType;
import edu.brown.workload.TransactionTrace;
import edu.brown.workload.Workload;
import edu.brown.workload.filters.ProcedureLimitFilter;
import edu.brown.workload.filters.ProcedureNameFilter;

public class TestFeatureClusterer extends BaseTestCase {

    private static final Class<? extends VoltProcedure> TARGET_PROCEDURE = neworder.class;
    private static final int WORKLOAD_XACT_LIMIT = 1000;
//    private static final int BASE_PARTITION = 1;
    private static final int NUM_PARTITIONS = 5;

    private static Procedure catalog_proc;
    private static Workload workload;
    private static FeatureSet fset;
    private static Instances data;
    private static ParameterCorrelations correlations;
    
    private FeatureClusterer fclusterer;
    
    @Override
    protected void setUp() throws Exception {
        super.setUp(ProjectType.TPCC);
        this.addPartitions(NUM_PARTITIONS);
        
        if (workload == null) {
            catalog_proc = this.getProcedure(TARGET_PROCEDURE);
            
            File file = this.getCorrelationsFile(ProjectType.TPCC);
            correlations = new ParameterCorrelations();
            correlations.load(file.getAbsolutePath(), catalog_db);

            file = this.getWorkloadFile(ProjectType.TPCC);
            workload = new Workload(catalog);

            // Check out this beauty:
            // (1) Filter by procedure name
            // (2) Filter on partitions that start on our BASE_PARTITION
            // (3) Filter to only include multi-partition txns
            // (4) Another limit to stop after allowing ### txns
            // Where is your god now???
            Workload.Filter filter = new ProcedureNameFilter()
                    .include(TARGET_PROCEDURE.getSimpleName())
//                    .attach(new ProcParameterValueFilter().include(1, new Long(5))) // D_ID
//                    .attach(new ProcParameterArraySizeFilter(CatalogUtil.getArrayProcParameters(catalog_proc).get(0), 10, ExpressionType.COMPARE_EQUAL))
//                    .attach(new BasePartitionTxnFilter(p_estimator, BASE_PARTITION))
//                    .attach(new MultiPartitionTxnFilter(p_estimator))
                    .attach(new ProcedureLimitFilter(WORKLOAD_XACT_LIMIT));
            workload.load(file.getAbsolutePath(), catalog_db, filter);
            assert(workload.getTransactionCount() > 0);
            
            // Now extract the FeatureSet that we will use in our tests
            Map<Procedure, FeatureSet> fsets = new FeatureExtractor(catalog_db, p_estimator).calculate(workload);
            fset = fsets.get(catalog_proc);
            data = fset.export(catalog_proc.getName());
        }
        assertNotNull(fset);
        assertNotNull(data);
        
        fclusterer = new FeatureClusterer(catalog_db, workload, correlations);
    }
    
    public Set<Attribute> prefix2attributes(String...prefixes) {
        Set<Attribute> attributes = new ListOrderedSet<Attribute>();
        for (String key : prefixes) {
            Integer idx = fset.getFeatureIndex(key);
            assertNotNull(key, idx);
            Attribute attribute = data.attribute(idx);
            assertNotNull(key + "=>" + idx, attribute);
            attributes.add(attribute);
        } // FOR
        return (attributes);
    }
    
    /**
     * testCreateAttributeSetFilter
     */
//    @Test
//    public void testCreateAttributeSetFilter() throws Exception {
//        // Test that we can create a filter from an AttributeSet
//        Set<Integer> idxs = fset.getFeatureIndexes(ParamArrayLengthFeature.class);
//        assertEquals(CatalogUtil.getArrayProcParameters(catalog_proc).size(), idxs.size());
//        AttributeSet aset = new AttributeSet(data, idxs);
//        assertEquals(idxs.size(), aset.size());
//        
//        Filter filter = aset.createFilter(data);
//        Instances newData = Filter.useFilter(data, filter);
//        for (int i = 0, cnt = newData.numInstances(); i < cnt; i++) {
//            Instance processed = newData.instance(i);
////            System.err.println(processed);
//            assertEquals(aset.size(), processed.numAttributes());
//        } // WHILE
//        assertEquals(data.numInstances(), newData.numInstances());
////        System.err.println("AttributeSet: " + aset);
//        
//    }
//    
//    /**
//     * testCreateClusterer
//     */
//    @Test
//    public void testCreateClusterer() throws Exception {
//        // Construct a simple AttributeSet that only contains the BasePartitionFeature
//        Integer txn_id_idx = fset.getFeatureIndex(FeatureUtil.getFeatureKeyPrefix(TransactionIdFeature.class));
//        assertNotNull(txn_id_idx);
//        Integer base_partition_idx = fset.getFeatureIndex(FeatureUtil.getFeatureKeyPrefix(BasePartitionFeature.class));
//        assertNotNull(base_partition_idx);
//        AttributeSet aset = new AttributeSet(data.attribute(base_partition_idx));
//        
//        AbstractClusterer clusterer = this.fclusterer.createClusterer(data, aset, catalog_proc);
//        assertNotNull(clusterer);
//        
//        // Make sure that each Txn gets mapped to the same cluster as its base partition
//        Map<Integer, Histogram> p_c_xref = new HashMap<Integer, Histogram>();
//        for (int i = 0, cnt = data.numInstances(); i < cnt; i++) {
//            Instance inst = data.instance(i);
//            assertNotNull(inst);
//            long txn_id = (long)inst.value(txn_id_idx);
//
//            TransactionTrace txn_trace = workload.getTransaction(txn_id);
//            assertNotNull(txn_trace);
//            Integer base_partition = p_estimator.getBasePartition(txn_trace);
//            assertNotNull(base_partition);
//            assertEquals(base_partition.intValue(), (int)inst.value(base_partition_idx));
//
//            int c = clusterer.clusterInstance(inst);
//            if (p_c_xref.containsKey(base_partition) == false) {
//                p_c_xref.put(base_partition, new Histogram());
//            }
//            p_c_xref.get(base_partition).put(c);
//        } // FOR
//        
//        
////        System.err.println(StringUtil.formatMaps(p_c_xref));
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
//    }

    /**
     * 
     * @throws Exception
     */
    @Test
    public void testCreateAttributeSet() throws Exception {
        Set<Attribute> attributes = prefix2attributes(
//            FeatureUtil.getFeatureKeyPrefix(BasePartitionFeature.class),
            FeatureUtil.getFeatureKeyPrefix(ParamArrayLengthFeature.class, this.getProcParameter(catalog_proc, 4)),
            FeatureUtil.getFeatureKeyPrefix(ParamNumericValuesFeature.class, this.getProcParameter(catalog_proc, 1))
        );
        System.err.println("Attributes: " + attributes);
        
        Pair<Instances, Instances> p = fclusterer.splitWorkload(data);
        assertNotNull(p);
        
        for (int i = 0; i < 1; i++) {
            AttributeSet aset = fclusterer.createAttributeSet(catalog_proc, attributes, p.getFirst(), p.getSecond());
//            System.err.println("[" + i + "] Cost: " + aset.getCost());
        }
//        System.err.println();
        
//        attributes = prefix2attributes(
//            FeatureUtil.getFeatureKeyPrefix(BasePartitionFeature.class),
//            FeatureUtil.getFeatureKeyPrefix(ParamNumericValuesFeature.class, this.getProcParameter(catalog_proc, 1))
//        );
//        System.err.println("Attributes: " + attributes);
//        for (int i = 0; i < 4; i++) {
//            AttributeSet aset = fclusterer.createAttributeSet(catalog_proc, attributes, p.getFirst(), p.getSecond());
//            System.err.println("[" + i + "] Cost: " + aset.getCost());
//        }
    }

}