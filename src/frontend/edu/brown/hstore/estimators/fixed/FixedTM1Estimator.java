package edu.brown.hstore.estimators.fixed;

import java.util.HashSet;
import java.util.Set;

import org.voltdb.catalog.Procedure;

import edu.brown.hstore.estimators.EstimatorState;
import edu.brown.utils.PartitionEstimator;
import edu.brown.utils.PartitionSet;

/**
 * TM1 Benchmark Fixed Estimator
 * @author pavlo
 */
public class FixedTM1Estimator extends AbstractFixedEstimator {

    private final Set<String> singlePartition = new HashSet<String>();
    private final Set<String> multiPartition = new HashSet<String>();
    private final Set<String> readOnly = new HashSet<String>();
    
    public FixedTM1Estimator(PartitionEstimator p_estimator) {
        super(p_estimator);
        
        // NOTE: We can't use the class references here because
        // they won't be in our build path at compile time. So we'll have
        // to rely on the just names
        
        String spClasses[] = {
           "GetAccessData",
           "GetNewDestination",
           "GetSubscriberData",
           "UpdateSubscriberData",
           
           // When the secondary index is installed with speculative execution,
           // it is faster to execute all txns as single-partitioned and then restart them
           // when they need to access a remote partition
           "DeleteCallForwarding",
           "InsertCallForwarding",
           "UpdateLocation",
        };
        for (String procName : spClasses) {
            assert(this.catalogContext.procedures.get(procName) != null) :
                "Unexpected procedure name '" + procName + "'";
            this.singlePartition.add(procName);
            if (procName.startsWith("Get")) this.readOnly.add(procName);
        } // FOR
        
        String mpClasses[] = {
//           "DeleteCallForwarding",
//           "InsertCallForwarding",
//           "UpdateLocation",
        };
        for (String procName : mpClasses) {
            assert(this.catalogContext.procedures.get(procName) != null) :
                "Unexpected procedure name '" + procName + "'";
            this.multiPartition.add(procName);
        } // FOR
    }
    
    @SuppressWarnings("unchecked")
    @Override
    public <T extends EstimatorState> T startTransactionImpl(Long txn_id, int base_partition, Procedure catalog_proc, Object[] args) {
        String procName = catalog_proc.getName();
        FixedEstimatorState ret = new FixedEstimatorState(this.catalogContext, txn_id, base_partition);
        PartitionSet partitions = null;
        PartitionSet readonly = EMPTY_PARTITION_SET;
        
        // Single-Partition Transactions
        if (this.singlePartition.contains(procName)) {
            partitions = this.catalogContext.getPartitionSetSingleton(base_partition);
            if (this.readOnly.contains(procName)) readonly = partitions;
        }
        // Multi-Partition Transactions
        else {
            partitions = this.catalogContext.getAllPartitionIds();
        }
        
        ret.createInitialEstimate(partitions, readonly, EMPTY_PARTITION_SET);
        return ((T)ret);
    }
}
