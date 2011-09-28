package edu.brown.workload.filters;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

import org.voltdb.catalog.CatalogType;
import org.voltdb.catalog.Database;
import org.voltdb.catalog.Procedure;

import edu.brown.utils.PartitionEstimator;
import edu.brown.workload.AbstractTraceElement;
import edu.brown.workload.TransactionTrace;

public class BasePartitionTxnFilter extends Filter {
    
    private final PartitionEstimator p_estimator;
    private final Database catalog_db;
    private final Set<Integer> base_partitions = new HashSet<Integer>();
    
    public BasePartitionTxnFilter(PartitionEstimator p_estimator, int...base_partitions) {
        super();
        this.p_estimator = p_estimator;
        this.catalog_db = p_estimator.getDatabase();
        
        for (int p : base_partitions) {
            this.base_partitions.add(p);
        }
    }
    
    public void addPartitions(Collection<Integer> p) {
        this.base_partitions.addAll(p);
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
                partition = this.p_estimator.getBasePartition(catalog_proc, xact.getParams(), true);
            } catch (Exception ex) {
                ex.printStackTrace();
                assert(false);
            }
            assert(partition >= 0);
            return (this.base_partitions.contains(partition) ? FilterResult.ALLOW : FilterResult.SKIP);
        }
        return FilterResult.ALLOW;
    }
    
    @Override
    public String debugImpl() {
        return null;
    }

}
