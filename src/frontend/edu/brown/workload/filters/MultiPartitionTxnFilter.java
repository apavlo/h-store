package edu.brown.workload.filters;

import java.util.HashSet;
import java.util.Set;

import org.voltdb.catalog.CatalogType;
import org.voltdb.catalog.Database;
import org.voltdb.catalog.Procedure;

import edu.brown.utils.PartitionEstimator;
import edu.brown.workload.AbstractTraceElement;
import edu.brown.workload.QueryTrace;
import edu.brown.workload.TransactionTrace;

public class MultiPartitionTxnFilter extends Filter {
    
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
        this.catalog_db = p_estimator.getDatabase();
        this.singlepartition = singlepartition;
    }
    
    public MultiPartitionTxnFilter(PartitionEstimator p_estimator) {
        this(p_estimator, false);
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
            
            Set<Integer> partitions = new HashSet<Integer>();
            try {
                int base_partition = this.p_estimator.getBasePartition(catalog_proc, xact.getParams(), true);
                partitions.add(base_partition);
                
                for (QueryTrace query : xact.getQueries()) {
                    partitions.addAll(this.p_estimator.getAllPartitions(query, base_partition));
                } // FOR
                
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
            assert(partitions.isEmpty() == false);
            boolean allow = (this.singlepartition ? (partitions.size() == 1) : (partitions.size() > 1)); 
            return  (allow ? FilterResult.ALLOW : FilterResult.SKIP);
            
        }
        return FilterResult.ALLOW;
    }
    
    @Override
    public String debugImpl() {
        return null;
    }

}
