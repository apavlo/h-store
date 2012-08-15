package edu.brown.markov;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.junit.Test;
import org.voltdb.VoltProcedure;
import org.voltdb.benchmark.tpcc.procedures.neworder;
import org.voltdb.catalog.Procedure;

import weka.core.Instance;
import weka.core.Instances;
import edu.brown.BaseTestCase;
import edu.brown.utils.JSONUtil;
import edu.brown.utils.ProjectType;
import edu.brown.workload.TransactionTrace;
import edu.brown.workload.Workload;
import edu.brown.workload.filters.Filter;
import edu.brown.workload.filters.ProcedureLimitFilter;
import edu.brown.workload.filters.ProcedureNameFilter;

public class TestFeatureExtractor extends BaseTestCase {

    private static final Class<? extends VoltProcedure> TARGET_PROCEDURE = neworder.class;
    private static final int WORKLOAD_XACT_LIMIT = 4;

    private static Procedure catalog_proc;
    private static Workload workload;
    private static Instances data;
    
    @Override
    protected void setUp() throws Exception {
        super.setUp(ProjectType.TPCC);
        
        if (workload == null) {
            catalog_proc = this.getProcedure(TARGET_PROCEDURE);
            
            File file = this.getWorkloadFile(ProjectType.TPCC);
            workload = new Workload(catalog);
            Filter filter = new ProcedureNameFilter(false)
                    .include(TARGET_PROCEDURE.getSimpleName())
                    .attach(new ProcedureLimitFilter(WORKLOAD_XACT_LIMIT));
            workload.load(file, catalog_db, filter);
            assert(workload.getTransactionCount() > 0);
            
            // Now extract the FeatureSet that we will use in our tests
            Map<Procedure, FeatureSet> fsets = new FeatureExtractor(catalogContext, p_estimator).calculate(workload);
            FeatureSet fset = fsets.get(catalog_proc);
            assertNotNull(fset);
            System.err.println(JSONUtil.format(fset.toJSONString()));
            data = fset.export(catalog_proc.getName());
        }
        assertNotNull(data);
    }

    /**
     * testTransactionLookup
     */
    @Test
    public void testTransactionLookup() throws Exception {
        int txn_id_idx = FeatureExtractor.TXNID_ATTRIBUTE_IDX;
        assertEquals(workload.getTransactionCount(), data.numInstances());
        List<TransactionTrace> txns = new ArrayList<TransactionTrace>(workload.getTransactions());
//        System.err.println(StringUtil.join("\n", txns));
//        System.err.println();
        for (int i = 0, cnt = data.numInstances(); i < cnt; i++) {
            Instance inst = data.instance(i);
            assertNotNull(inst);
            
            String value = inst.stringValue(txn_id_idx);
//            System.err.println("VALUE:    " + value);
            Long txn_id = Long.valueOf(value);
            assertNotNull(txn_id);
            
            TransactionTrace txn_trace = workload.getTransaction(txn_id);
            TransactionTrace expected = txns.get(i);
//            System.err.println("EXPECTED: " + expected.getTransactionId());
//            System.err.println("FOUND:    " + txn_id);
            
            assertNotNull(String.format("[%05d] Failed to txn #%d", i, txn_id), txn_trace);
            assertEquals(expected.getTransactionId(), txn_trace.getTransactionId());
        } // FOR
    }

}