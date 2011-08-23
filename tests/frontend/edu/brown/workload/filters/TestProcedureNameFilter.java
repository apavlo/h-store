package edu.brown.workload.filters;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import org.junit.Test;
import org.voltdb.benchmark.tpcc.procedures.delivery;
import org.voltdb.benchmark.tpcc.procedures.neworder;
import org.voltdb.benchmark.tpcc.procedures.slev;
import org.voltdb.catalog.CatalogType;

import edu.brown.workload.AbstractTraceElement;
import edu.brown.workload.TransactionTrace;

/**
 * @author pavlo
 */
public class TestProcedureNameFilter extends AbstractTestFilter {

    /**
     * testExclude
     */
    @Test
    public void testExclude() throws Exception {
        String exclude = neworder.class.getSimpleName();
        ProcedureNameFilter filter = new ProcedureNameFilter(false).exclude(exclude);
        
        Iterator<TransactionTrace> it = workload.iterator(filter);
        assertNotNull(it);

        int count = 0;
        while (it.hasNext()) {
            AbstractTraceElement<? extends CatalogType> element = it.next();
            if (element instanceof TransactionTrace) {
                assertNotSame(exclude, element.getCatalogItemName());
                count++;
            }
        } // WHILE
        assert(count > 0);
    }
    
    /**
     * testInclude
     */
    @Test
    public void testInclude() throws Exception {
        String include = slev.class.getSimpleName();
        ProcedureNameFilter filter = new ProcedureNameFilter(false).include(include);
        
        Iterator<TransactionTrace> it = workload.iterator(filter);
        assertNotNull(it);

        int count = 0;
        while (it.hasNext()) {
            AbstractTraceElement<? extends CatalogType> element = it.next();
            if (element instanceof TransactionTrace) {
                assertEquals(include, element.getCatalogItemName());
                count++;
            }
        } // WHILE
        assert(count > 0);
    }
    
    /**
     * testMultiInclude
     */
    @Test
    public void testMultiInclude() throws Exception {
        Map<String, Integer> expected = new HashMap<String, Integer>();
        expected.put(slev.class.getSimpleName(), 10);
        expected.put(delivery.class.getSimpleName(), 15);
        expected.put(neworder.class.getSimpleName(), 20);
        
        ProcedureNameFilter filter = new ProcedureNameFilter(false);
        Map<String, Integer> results = new HashMap<String, Integer>();
        for (Entry<String, Integer> e : expected.entrySet()) {
            filter.include(e.getKey(), e.getValue());
            results.put(e.getKey(), 0);
        } // FOR
        
        Iterator<TransactionTrace> it = workload.iterator(filter);
        assertNotNull(it);

        int count = 0;
        while (it.hasNext()) {
            TransactionTrace element = it.next();
            String name = element.getCatalogItemName();
            assert(results.containsKey(name)) : "Unexpected " + name;
            results.put(name, results.get(name) + 1);
            count++;
        } // WHILE
        assert(count > 0);
        
        // Now check that we got the proper counts for all of our procedures
        for (Entry<String, Integer> e : expected.entrySet()) {
            Integer cnt = results.get(e.getKey());
            // System.err.println(e.getKey() + " => " + cnt);
            assertEquals(e.getKey(), e.getValue(), cnt);
        } // FOR
    }
}