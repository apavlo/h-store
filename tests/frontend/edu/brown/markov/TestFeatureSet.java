package edu.brown.markov;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.Vector;
import java.util.Map.Entry;

import org.json.JSONObject;
import org.junit.Test;
import org.voltdb.VoltProcedure;
import org.voltdb.benchmark.tpcc.procedures.neworder;
import org.voltdb.catalog.Procedure;

import weka.core.Instances;
import edu.brown.BaseTestCase;
import edu.brown.markov.FeatureSet.Type;
import edu.brown.markov.features.AbstractFeature;
import edu.brown.markov.features.BasePartitionFeature;
import edu.brown.markov.features.ParamArrayLengthFeature;
import edu.brown.markov.features.ParamHashEqualsBasePartitionFeature;
import edu.brown.statistics.Histogram;
import edu.brown.statistics.ObjectHistogram;
import edu.brown.utils.CollectionUtil;
import edu.brown.utils.ProjectType;
import edu.brown.workload.TransactionTrace;
import edu.brown.workload.Workload;
import edu.brown.workload.filters.Filter;
import edu.brown.workload.filters.ProcedureLimitFilter;
import edu.brown.workload.filters.ProcedureNameFilter;

public class TestFeatureSet extends BaseTestCase {

    private static final Class<? extends VoltProcedure> TARGET_PROCEDURE = neworder.class;
    private static final int WORKLOAD_XACT_LIMIT = 10;
    
    private static Workload workload;
    private Procedure catalog_proc;
    private final Random rand = new Random();
    private FeatureSet fset;
    private TransactionTrace txn_trace;
    
    @Override
    protected void setUp() throws Exception {
        super.setUp(ProjectType.TPCC);
        
        if (workload == null) {
            File file = this.getWorkloadFile(ProjectType.TPCC);
            workload = new Workload(catalog);
            Filter filter = new ProcedureNameFilter(false).include(TARGET_PROCEDURE.getSimpleName())
                                         .attach(new ProcedureLimitFilter(WORKLOAD_XACT_LIMIT));
            workload.load(file, catalog_db, filter);
        }
        
        this.catalog_proc = this.getProcedure(TARGET_PROCEDURE);
        this.fset = new FeatureSet();
        this.txn_trace = CollectionUtil.first(workload.getTransactions());
    }
    
    /**
     * testAddFeature
     */
    @Test
    public void testAddFeature() throws Exception {
        List<String> orig_keys = new ArrayList<String>();
        orig_keys.add("KEY1");
        orig_keys.add("KEY2");
        orig_keys.add("KEY3");
        for (String k : orig_keys) {
            Integer val = rand.nextInt();
            this.fset.addFeature(this.txn_trace, k, val);
        } // FOR
        
        List<String> features = fset.getFeatures();
        assert(features.isEmpty() == false);
        assertEquals(orig_keys.size(), features.size());
        assertEquals(orig_keys, features);
    }
    
    /**
     * testExport
     */
    @Test
    public void testExport() throws Exception {
        List<String> orig_keys = new ArrayList<String>();
        orig_keys.add("KEY1");
        orig_keys.add("KEY2");
        orig_keys.add("KEY3");
        for (String k : orig_keys) {
            Integer val = rand.nextInt();
            this.fset.addFeature(this.txn_trace, k, val);
        } // FOR

        Instances data = this.fset.export(TARGET_PROCEDURE.getSimpleName());
        assertNotNull(data);
        String contents = data.toString();
        assertNotNull(contents);
        assertFalse(contents.isEmpty());

        for (String k : orig_keys) {
            assert(contents.contains(k));
        }
    }

    /**
     * testAddBooleanFeature
     */
    @Test
    public void testAddBooleanFeature() {
        Object values[] = { true, false, new Boolean(true) };
        for (int i = 0; i < values.length; i++) {
            String key = "KEY" + i;
            this.fset.addFeature(this.txn_trace, key, values[i]);    
        } // FOR
        
        for (int i = 0; i < values.length; i++) {
            String key = "KEY" + i;
            assertEquals(FeatureSet.Type.BOOLEAN, this.fset.getFeatureType(key));
        }
    }
    
    /**
     * testAddNumericFeature
     */
    @Test
    public void testAddNumericFeature() {
        Object values[] = { 1.0d, 1l, 1 };
        for (int i = 0; i < values.length; i++) {
            String key = "KEY" + i;
            this.fset.addFeature(this.txn_trace, key, values[i]);    
        } // FOR
        
        for (int i = 0; i < values.length; i++) {
            String key = "KEY" + i;
            assertEquals(FeatureSet.Type.NUMERIC, this.fset.getFeatureType(key));
        } // FOR
    }

    /**
     * testSerialization
     */
    @Test
    public void testSerialization() throws Exception {
        AbstractFeature features[] = new AbstractFeature[] {
            new BasePartitionFeature(p_estimator, catalog_proc),
            new ParamArrayLengthFeature(p_estimator, catalog_proc),
            new ParamHashEqualsBasePartitionFeature(p_estimator, catalog_proc),
        };
        for (TransactionTrace txn_trace : workload.getTransactions()) {
            for (AbstractFeature f : features) {
                f.extract(this.fset, txn_trace);
            } // FOR
        } // FOR

        String json = this.fset.toJSONString();
        assertNotNull(json);
        assertFalse(json.isEmpty());
//        System.err.println(JSONUtil.format(json));
        
        FeatureSet clone = new FeatureSet();
        clone.fromJSON(new JSONObject(json), catalog_db);
        
        for (Entry<String, Type> e : this.fset.attributes.entrySet()) {
            assertEquals(e.getKey(), e.getValue(), clone.attributes.get(e.getKey()));
        } // FOR
        for (Entry<String, ObjectHistogram> e : this.fset.attribute_histograms.entrySet()) {
            ObjectHistogram<?> clone_h = clone.attribute_histograms.get(e.getKey());
//            System.err.println(e.getValue());
//            System.err.println();
//            System.err.println(clone_h.isEmpty() ? "<EMPTY>" : clone_h.toString());
//            
//            System.err.println("\nEQUALS = " + e.getValue().equals(clone_h));
//            System.err.println("-------------------------------------------\n");
//            
//            for (Object o : e.getValue().values()) {
//                System.err.println("ORIG " + o + ": " + o.getClass());
//            }
//            for (Object o : clone_h.values()) {
//                System.err.println("CLONE " + o + ": " + o.getClass());
//            }
            
            assertEquals(e.getKey(), e.getValue(), clone_h);
        } // FOR
        for (Entry<Long, Vector<Object>> e : this.fset.txn_values.entrySet()) {
            Vector<Object> clone_v = clone.txn_values.get(e.getKey());
            assertNotNull(e.getKey().toString(), clone_v);
            assertEquals(e.getValue().size(), clone_v.size());
//            System.err.println("ORIG:  " + e.getValue());
//            System.err.println("CLONE: " + clone_v);
            assert(e.getValue().containsAll(clone_v));
        } // FOR
        assertEquals(this.fset.last_num_attributes, clone.last_num_attributes);
    }
    
}