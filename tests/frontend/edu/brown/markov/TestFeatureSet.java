package edu.brown.markov;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.junit.Test;
import org.voltdb.VoltProcedure;
import org.voltdb.benchmark.tpcc.procedures.neworder;
import org.voltdb.catalog.Procedure;

import edu.brown.BaseTestCase;
import edu.brown.utils.FileUtil;
import edu.brown.utils.ProjectType;
import edu.brown.workload.TransactionTrace;
import edu.brown.workload.Workload;
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
            Workload.Filter filter = new ProcedureNameFilter().include(TARGET_PROCEDURE.getSimpleName())
                                         .attach(new ProcedureLimitFilter(WORKLOAD_XACT_LIMIT));
            workload.load(file.getAbsolutePath(), catalog_db, filter);
        }
        
        this.catalog_proc = this.getProcedure(TARGET_PROCEDURE);
        this.fset = new FeatureSet();
        this.txn_trace = workload.getTransactions().get(0);
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
     * testSave
     */
    @Test
    public void testSave() throws Exception {
        List<String> orig_keys = new ArrayList<String>();
        orig_keys.add("KEY1");
        orig_keys.add("KEY2");
        orig_keys.add("KEY3");
        for (String k : orig_keys) {
            Integer val = rand.nextInt();
            this.fset.addFeature(this.txn_trace, k, val);
        } // FOR
        
        String path = "/tmp/fset.txt";
        this.fset.save(path, TARGET_PROCEDURE.getSimpleName());
        String contents = FileUtil.readFile(path);
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
        }
    }

    
}
