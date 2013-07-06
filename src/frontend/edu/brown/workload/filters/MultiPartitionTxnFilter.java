package edu.brown.workload.filters;

import org.apache.log4j.Logger;
import org.voltdb.catalog.CatalogType;
import org.voltdb.catalog.Database;
import org.voltdb.catalog.Procedure;

import edu.brown.logging.LoggerUtil.LoggerBoolean;
import edu.brown.utils.PartitionEstimator;
import edu.brown.utils.PartitionSet;
import edu.brown.workload.AbstractTraceElement;
import edu.brown.workload.QueryTrace;
import edu.brown.workload.TransactionTrace;

/**
 * Filter TranasctionTraces based on whether they are single-partitioned or not
 * @author pavlo
 */
public class MultiPartitionTxnFilter extends Filter {
    private static final Logger LOG = Logger.getLogger(MultiPartitionTxnFilter.class);
    private static final LoggerBoolean debug = new LoggerBoolean();
    
    private final PartitionEstimator p_estimator;
    private final Database catalog_db;
    private final boolean singlepartition;

    /**
     * Constructor
     * @param p_estimator
     * @param singlepartition - if true, only allow singlepartition txns, otherwise only multipartition txns
     */
    public MultiPartitionTxnFilter(PartitionEstimator p_estimator, boolean singlepartition) {
        super();
        this.p_estimator = p_estimator;
        this.catalog_db = p_estimator.getCatalogContext().database;
        this.singlepartition = singlepartition;
    }

    @Override
    protected void resetImpl() {
        // Ignore...
    }
    
    @Override
    protected FilterResult filter(AbstractTraceElement<? extends CatalogType> element) {
        FilterResult result = FilterResult.ALLOW;
        if (element instanceof TransactionTrace) {
            TransactionTrace xact = (TransactionTrace)element;
            Procedure catalog_proc = xact.getCatalogItem(this.catalog_db);
            
            PartitionSet partitions = new PartitionSet();
            try {
                int base_partition = this.p_estimator.getBasePartition(catalog_proc, xact.getParams(), true);
                partitions.add(base_partition);
                for (QueryTrace query : xact.getQueries()) {
                    this.p_estimator.getAllPartitions(partitions, query, base_partition);
                } // FOR
                
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
            assert(partitions.isEmpty() == false);
            boolean allow = (this.singlepartition ? (partitions.size() == 1) : (partitions.size() > 1));
            result = (allow ? FilterResult.ALLOW : FilterResult.SKIP);
            if (debug.val)
                LOG.debug(String.format("%s :: partitions=%s / allow=%s ==> %s",
                          xact, partitions, allow, result));
        }
        return (result);
    }
    
    @Override
    public String debugImpl() {
        return (this.getClass().getSimpleName() + ": singlePartition=" + this.singlepartition);
    }

}
