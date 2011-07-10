package edu.brown.workload.filters;

import java.util.Iterator;

import org.junit.Test;
import org.voltdb.catalog.CatalogType;

import edu.brown.workload.AbstractTraceElement;
import edu.brown.workload.TransactionTrace;

/**
 * @author pavlo
 */
public class TestProcedureLimitFilter extends AbstractTestFilter {

    /**
     * testFilter
     */
    @Test
    public void testFilter() throws Exception {
        int new_limit = 100;
        ProcedureLimitFilter filter = new ProcedureLimitFilter(new_limit);
        
        Iterator<TransactionTrace> it = workload.iterator(filter);
        assertNotNull(it);

        int count = 0;
        while (it.hasNext()) {
            AbstractTraceElement<? extends CatalogType> element = it.next();
            if (element instanceof TransactionTrace) {
                count++;
            }
        } // WHILE
        assertEquals(new_limit, count);
    }
}