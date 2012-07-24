package edu.brown.workload.filters;

import java.util.Iterator;

import org.junit.Test;
import org.voltdb.catalog.CatalogType;

import edu.brown.hstore.HStoreConstants;
import edu.brown.workload.AbstractTraceElement;
import edu.brown.workload.TransactionTrace;

/**
 * @author pavlo
 */
public class TestBasePartitionTxnFilter extends AbstractTestFilter {
    
    /**
     * testFilter
     */
    @Test
    public void testFilter() throws Exception {
        BasePartitionTxnFilter filter = new BasePartitionTxnFilter(p_estimator, BASE_PARTITION);
        
        Iterator<TransactionTrace> it = workload.iterator(filter);
        assertNotNull(it);

        int count = 0;
        while (it.hasNext()) {
            AbstractTraceElement<? extends CatalogType> element = it.next();
            if (element instanceof TransactionTrace) {
                // Make sure that this txn's base partition is what we expect it to be
                TransactionTrace txn = (TransactionTrace)element;
                int base_partition = p_estimator.getBasePartition(txn.getCatalogItem(catalog_db), txn.getParams(), true);
                assert(base_partition != HStoreConstants.NULL_PARTITION_ID);
                assertEquals(BASE_PARTITION, base_partition);
                count++;
            }
        } // WHILE
        assert(count > 0);
    }
}