package edu.brown.workload.filters;

import java.util.Iterator;

import org.junit.Test;
import org.voltdb.benchmark.tpcc.procedures.neworder;
import org.voltdb.catalog.CatalogType;
import org.voltdb.catalog.ProcParameter;
import org.voltdb.catalog.Procedure;

import edu.brown.workload.AbstractTraceElement;
import edu.brown.workload.TransactionTrace;

/**
 * @author pavlo
 */
public class TestProcParameterValueFilter extends AbstractTestFilter {

    private static final String TARGET_PROCEDURE = neworder.class.getSimpleName();
    private static final int PARAM_IDX = 1; // D_ID
    private static final Object PARAM_VALUE = new Integer(5);
    
    private Procedure catalog_proc;
    private ProcParameter catalog_param;
    
    @Override
    protected void setUp() throws Exception {
        super.setUp();
        this.catalog_proc = this.getProcedure(TARGET_PROCEDURE);
        this.catalog_param = this.getProcParameter(this.catalog_proc, PARAM_IDX);
    }
    
    /**
     * testFilter
     */
    @Test
    public void testFilter() throws Exception {
        Filter filter = new ProcedureNameFilter(false).include(TARGET_PROCEDURE)
            .attach(new ProcParameterValueFilter().include(this.catalog_param, PARAM_VALUE));
        
        Iterator<TransactionTrace> it = workload.iterator(filter);
        assertNotNull(it);

        int count = 0;
        while (it.hasNext()) {
            AbstractTraceElement<? extends CatalogType> element = it.next();
            if (element instanceof TransactionTrace) {
                // Make sure that all of the array parameters have the same size
                TransactionTrace txn = (TransactionTrace)element;
                assertEquals(TARGET_PROCEDURE, txn.getCatalogItemName());
                Object val = txn.getParam(PARAM_IDX);
                assertNotNull(val);
                assertEquals(txn.toString(), PARAM_VALUE, val);
                count++;
            }
        } // WHILE
        assert(count > 0);
    }
}