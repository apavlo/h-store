package edu.brown.workload.filters;

import org.voltdb.catalog.CatalogType;
import org.voltdb.catalog.Database;
import org.voltdb.catalog.Procedure;

import edu.brown.utils.PartitionEstimator;
import edu.brown.workload.AbstractTraceElement;
import edu.brown.workload.Workload;
import edu.brown.workload.TransactionTrace;

public class BasePartitionTxnFilter extends Workload.Filter {
    
    private final PartitionEstimator p_estimator;
    private final Database catalog_db;
    private final int base_partition;
    
    public BasePartitionTxnFilter(PartitionEstimator p_estimator, int base_partition) {
        super();
        this.p_estimator = p_estimator;
        this.base_partition = base_partition;
        this.catalog_db = p_estimator.getDatabase();
    }
    
    @Override
    protected void resetImpl() {
        // Ignore...
    }
    
    @Override
    protected FilterResult filter(AbstractTraceElement<? extends CatalogType> element) {
        if (element instanceof TransactionTrace) {
            TransactionTrace xact = (TransactionTrace)element;
            Procedure catalog_proc = xact.getCatalogItem(this.catalog_db);
            int partition = -1;
            try {
                partition = this.p_estimator.getPartition(catalog_proc, xact.getParams(), true);
            } catch (Exception ex) {
                ex.printStackTrace();
                assert(false);
            }
            assert(partition >= 0);
            return (partition == base_partition ? FilterResult.ALLOW : FilterResult.SKIP);
        }
        return FilterResult.ALLOW;
    }
    
    @Override
    protected String debug() {
        return null;
    }

}
