package edu.brown.markov.features;

import java.util.List;
import java.util.Random;

import org.junit.Test;
import org.voltdb.VoltProcedure;
import org.voltdb.benchmark.tpcc.procedures.neworder;
import org.voltdb.catalog.ProcParameter;
import org.voltdb.catalog.Procedure;
import org.voltdb.types.TimestampType;

import edu.brown.BaseTestCase;
import edu.brown.markov.FeatureSet;
import edu.brown.utils.ProjectType;
import edu.brown.workload.TransactionTrace;

public class TestFeatures extends BaseTestCase {
    
    private static final Class<? extends VoltProcedure> TARGET_PROCEDURE = neworder.class;
    private static final int NUM_ITEMS = 10;
    private static final Random rand = new Random();

    private Procedure catalog_proc;
    private FeatureSet fset;
    private TransactionTrace txn_trace;
    
    private final Object params[] = {
        new Integer(1),     // (0) W_ID
        new Integer(2),     // (1) D_ID
        new Integer(3),     // (2) C_ID
        new TimestampType(),// (3) TIMESTAMP
        null,               // (4) ITEM_ID
        null,               // (5) SUPPY_WAREHOUSE
        null,               // (6) QUANTITY
    };
    
    
    @Override
    protected void setUp() throws Exception {
        super.setUp(ProjectType.TPCC);
        
        // Populate item values
        for (int i = 4; i <= 6; i++) {
            Long arr[] = new Long[rand.nextInt(NUM_ITEMS)];
            for (int ii = 0; ii < arr.length; ii++) {
                arr[ii] = rand.nextLong();
            } // FOR
            this.params[i] = arr;
        } // FOR
        
        this.catalog_proc = this.getProcedure(TARGET_PROCEDURE);
        this.txn_trace = new TransactionTrace(1234, this.catalog_proc, this.params);
        this.fset = new FeatureSet(); 
        
    }
    
    private void validate(AbstractFeature f, TransactionTrace txn, FeatureSet fset) throws Exception {
        List<String> features = fset.getFeatures();
        assertNotNull(features);
        List<Object> values = fset.getFeatureValues(txn);
        assertNotNull(values);
        assertEquals(features.size(), values.size());
        
        for (int i = 0, cnt = features.size(); i < cnt; i++) {
            String key = features.get(i);
            Object val = values.get(i);
            
            // Check to make sure that if we call calculate that we get the same value back
            Object calculated = f.calculate(key, txn);
            assertEquals(key, val.toString(), calculated.toString());
        } // FOR
    }

    /**
     * testParamArrayLengthFeature
     */
    @Test
    public void testParamArrayLengthFeature() throws Exception {
        ParamArrayLengthFeature f = new ParamArrayLengthFeature(p_estimator, this.catalog_proc);
        assertNotNull(f);
        f.extract(this.fset, this.txn_trace);
        this.validate(f, txn_trace, fset);

        for (ProcParameter catalog_param : this.catalog_proc.getParameters()) {
            String key = f.getFeatureKey(catalog_param);
            if (catalog_param.getIsarray()) {
                assert(fset.hasFeature(key)) : key;
                Long val = fset.getFeatureValue(this.txn_trace, key);
                assertEquals(key, ((Long[])params[catalog_param.getIndex()]).length, val.intValue());
            } else {
                assertFalse(key,fset.hasFeature(key));
            }
        } // FOR
    }
    
    /**
     * testParamNumericValuesFeature
     */
    @Test
    public void testParamNumericValuesFeature() throws Exception {
        ParamNumericValuesFeature f = new ParamNumericValuesFeature(p_estimator, this.catalog_proc);
        assertNotNull(f);
        f.extract(this.fset, this.txn_trace);
        this.validate(f, txn_trace, fset);
    }
    
    /**
     * testBasePartitionFeature
     */
    @Test
    public void testBasePartitionFeature() throws Exception {
        BasePartitionFeature f = new BasePartitionFeature(p_estimator, this.catalog_proc);
        assertNotNull(f);
        f.extract(this.fset, this.txn_trace);
        this.validate(f, txn_trace, fset);
    }
    
    /**
     * testParamArrayAllSameHashFeature
     */
    @Test
    public void testParamArrayAllSameHashFeature() throws Exception {
        ParamArrayAllSameHashFeature f = new ParamArrayAllSameHashFeature(p_estimator, this.catalog_proc);
        assertNotNull(f);
        f.extract(this.fset, this.txn_trace);
        this.validate(f, txn_trace, fset);
    }
    
    /**
     * testParamArrayAllSameValueFeature
     */
    @Test
    public void testParamArrayAllSameValueFeature() throws Exception {
        ParamArrayAllSameValueFeature f = new ParamArrayAllSameValueFeature(p_estimator, this.catalog_proc);
        assertNotNull(f);
        f.extract(this.fset, this.txn_trace);
        this.validate(f, txn_trace, fset);
    }
    
    /**
     * testParamHashEqualsBasePartitionFeature
     */
    @Test
    public void testParamHashEqualsBasePartitionFeature() throws Exception {
        ParamHashEqualsBasePartitionFeature f = new ParamHashEqualsBasePartitionFeature(p_estimator, this.catalog_proc);
        assertNotNull(f);
        f.extract(this.fset, this.txn_trace);
        this.validate(f, txn_trace, fset);
    }
    
    /**
     * testParamHashPartitionFeature
     */
    @Test
    public void testParamHashPartitionFeature() throws Exception {
        ParamHashPartitionFeature f = new ParamHashPartitionFeature(p_estimator, this.catalog_proc);
        assertNotNull(f);
        f.extract(this.fset, this.txn_trace);
        this.validate(f, txn_trace, fset);
    }
    
}
