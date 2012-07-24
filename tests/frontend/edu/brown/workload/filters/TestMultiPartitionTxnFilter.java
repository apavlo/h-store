package edu.brown.workload.filters;

import java.util.Iterator;

import org.junit.Test;
import org.voltdb.catalog.CatalogType;

import edu.brown.utils.PartitionSet;
import edu.brown.workload.AbstractTraceElement;
import edu.brown.workload.TransactionTrace;

/**
 * @author pavlo
 */
public class TestMultiPartitionTxnFilter extends AbstractTestFilter {
    
    private final PartitionSet partitions = new PartitionSet();
    
    /**
     * testSinglePartition
     */
    @Test
    public void testSinglePartition() throws Exception {
        MultiPartitionTxnFilter filter = new MultiPartitionTxnFilter(p_estimator, true);
        
        Iterator<TransactionTrace> it = workload.iterator(filter);
        assertNotNull(it);

        int count = 0;
        while (it.hasNext()) {
            AbstractTraceElement<? extends CatalogType> element = it.next();
            if (element instanceof TransactionTrace) {
                // Make sure that this txn's base partition is what we expect it to be
                TransactionTrace txn = (TransactionTrace)element;
                partitions.clear();
                p_estimator.getAllPartitions(partitions, txn);
                assertNotNull(partitions);
                assertEquals(1, partitions.size());
                count++;
            }
        } // WHILE
        assert(count > 0);
    }
    
    /**
     * testMultiPartition
     */
    @Test
    public void testMultiPartition() throws Exception {
        MultiPartitionTxnFilter filter = new MultiPartitionTxnFilter(p_estimator, false);
        
        Iterator<TransactionTrace> it = workload.iterator(filter);
        assertNotNull(it);

        int count = 0;
        while (it.hasNext()) {
            AbstractTraceElement<? extends CatalogType> element = it.next();
            if (element instanceof TransactionTrace) {
                // Make sure that this txn's base partition is what we expect it to be
                TransactionTrace txn = (TransactionTrace)element;
                partitions.clear();
                p_estimator.getAllPartitions(partitions, txn);
                assertNotNull(partitions);
                assertNotSame(1, partitions.size());
                count++;
            }
        } // WHILE
        assert(count > 0);
    }
}