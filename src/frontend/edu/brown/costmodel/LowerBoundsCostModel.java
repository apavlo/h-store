package edu.brown.costmodel;

import org.apache.log4j.Logger;
import org.voltdb.CatalogContext;
import org.voltdb.catalog.Procedure;

import edu.brown.catalog.CatalogKey;
import edu.brown.utils.PartitionEstimator;
import edu.brown.workload.TransactionTrace;
import edu.brown.workload.filters.Filter;

public class LowerBoundsCostModel extends SingleSitedCostModel {
    private static final Logger LOG = Logger.getLogger(LowerBoundsCostModel.class);

    private int txn_ctr = 0;
    private final int num_partitions;

    public LowerBoundsCostModel(CatalogContext catalogContext, PartitionEstimator p_estimator) {
        super(catalogContext, p_estimator);
        this.num_partitions = catalogContext.numberOfPartitions;
    }

    @Override
    public void clear(boolean force) {
        super.clear(true);
        this.txn_ctr = 0;
    }

    @Override
    public TransactionCacheEntry processTransaction(final CatalogContext catalogContext, final TransactionTrace txn_trace, final Filter filter) throws Exception {
        final boolean trace = LOG.isTraceEnabled();

        // Make all transactions single-partition. Use round-robin partition
        // selection.
        Procedure catalog_proc = txn_trace.getCatalogItem(catalogContext.database);
        String proc_key = CatalogKey.createKey(catalog_proc);
        int total_queries = txn_trace.getQueryCount();
        int base_partition = (this.txn_ctr++ % this.num_partitions);

        if (trace)
            LOG.trace("Examining " + txn_trace + " [base_partition=" + base_partition + "]");

        TransactionCacheEntry txn_entry = this.createTransactionCacheEntry(txn_trace, proc_key);
        this.setBasePartition(txn_entry, base_partition);
        this.histogram_sp_procs.put(proc_key);

        txn_entry.setExaminedQueryCount(total_queries);
        txn_entry.setSingleSiteQueryCount(total_queries);
        txn_entry.setSingleSited(true);
        this.histogram_txn_partitions.put(base_partition);

        // For each query, we need to mark the histograms appropriately
        for (int i = 0; i < total_queries; i++) {
            this.histogram_query_partitions.put(base_partition);
            txn_entry.addTouchedPartition(base_partition);
        } // FOR

        return (txn_entry);
    }
} // END CLASS