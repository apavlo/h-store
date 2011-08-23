package edu.brown.workload.filters;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.junit.Test;
import org.voltdb.benchmark.tpcc.procedures.neworder;
import org.voltdb.catalog.CatalogType;
import org.voltdb.catalog.ProcParameter;
import org.voltdb.catalog.Procedure;
import org.voltdb.types.ExpressionType;

import edu.brown.utils.ClassUtil;
import edu.brown.workload.AbstractTraceElement;
import edu.brown.workload.TransactionTrace;

/**
 * @author pavlo
 */
public class TestProcParameterArraySizeFilter extends AbstractTestFilter {

    private static final String TARGET_PROCEDURE = neworder.class.getSimpleName(); 
    private static final int ARRAY_SIZE = 10;
    
    private Procedure catalog_proc;
    private List<ProcParameter> array_params = new ArrayList<ProcParameter>();
    
    @Override
    protected void setUp() throws Exception {
        super.setUp();
        
        this.catalog_proc = this.getProcedure(TARGET_PROCEDURE);
        for (ProcParameter param : this.catalog_proc.getParameters()) {
            if (param.getIsarray()) this.array_params.add(param);
        } // FOR
        assert(this.array_params.size() > 0);
    }
    
    private Filter makeFilter(ExpressionType exp_type) {
        Filter filter = new ProcedureNameFilter(false).include(TARGET_PROCEDURE)
            .attach(new ProcParameterArraySizeFilter(this.array_params.get(0), ARRAY_SIZE, exp_type));
        return (filter);
    }
    
    /**
     * testEquals
     */
    @Test
    public void testEquals() throws Exception {
        Filter filter = this.makeFilter(ExpressionType.COMPARE_EQUAL);
        Iterator<TransactionTrace> it = workload.iterator(filter);
        assertNotNull(it);

        int count = 0;
        while (it.hasNext()) {
            AbstractTraceElement<? extends CatalogType> element = it.next();
            if (element instanceof TransactionTrace) {
                // Make sure that all of the array parameters have the same size
                TransactionTrace txn = (TransactionTrace)element;
                assertEquals(TARGET_PROCEDURE, txn.getCatalogItemName());
                for (ProcParameter param : this.array_params) {
                    int param_idx = param.getIndex();
                    assert(param_idx < txn.getParamCount());
                    Object txn_param = txn.getParam(param_idx);
                    assert(ClassUtil.isArray(txn_param)) : "Param Idx #" + param_idx + "\n" + txn.debug(catalog_db);
                    assertEquals(ARRAY_SIZE, ((Object[])txn_param).length);
                } // FOR
                count++;
            }
        } // WHILE
        assert(count > 0);
    }
    
    /**
     * testLessThan
     */
    @Test
    public void testLessThan() throws Exception {
        Filter filter = this.makeFilter(ExpressionType.COMPARE_LESSTHAN);
        Iterator<TransactionTrace> it = workload.iterator(filter);
        assertNotNull(it);

        int count = 0;
        while (it.hasNext()) {
            AbstractTraceElement<? extends CatalogType> element = it.next();
            if (element instanceof TransactionTrace) {
                // Make sure that all of the array parameters have the same size
                TransactionTrace txn = (TransactionTrace)element;
                assertEquals(TARGET_PROCEDURE, txn.getCatalogItemName());
                for (ProcParameter param : this.array_params) {
                    int param_idx = param.getIndex();
                    assert(param_idx < txn.getParamCount());
                    Object txn_param = txn.getParam(param_idx);
                    assert(ClassUtil.isArray(txn_param)) : "Param Idx #" + param_idx + "\n" + txn.debug(catalog_db);
                    assert(((Object[])txn_param).length < ARRAY_SIZE);
                } // FOR
                count++;
            }
        } // WHILE
        assert(count > 0);
    }
}