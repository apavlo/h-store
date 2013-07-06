package edu.brown.hstore.estimators.fixed;

import org.apache.log4j.Logger;
import org.voltdb.VoltType;
import org.voltdb.catalog.Procedure;

import edu.brown.hstore.estimators.EstimatorState;
import edu.brown.logging.LoggerUtil;
import edu.brown.logging.LoggerUtil.LoggerBoolean;
import edu.brown.utils.PartitionEstimator;
import edu.brown.utils.PartitionSet;

/**
 * SEATS Benchmark Fixed Estimator
 * @author pavlo
 */
public class FixedSEATSEstimator extends AbstractFixedEstimator {
    private static final Logger LOG = Logger.getLogger(FixedSEATSEstimator.class);
    private static final LoggerBoolean debug = new LoggerBoolean();
    static {
        LoggerUtil.attachObserver(LOG, debug);
    }
    
    /**
     * Constructor
     * @param hstore_site
     */
    public FixedSEATSEstimator(PartitionEstimator p_estimator) {
        super(p_estimator);
    }
    
    @SuppressWarnings("unchecked")
    @Override
    public <T extends EstimatorState> T startTransactionImpl(Long txn_id, int base_partition, Procedure catalog_proc, Object[] args) {
        FixedEstimatorState ret = new FixedEstimatorState(this.catalogContext, txn_id, base_partition);
        String procName = catalog_proc.getName();
        Long f_id = null;
        Long c_id = null;
        PartitionSet partitions = null;
        PartitionSet readonly = EMPTY_PARTITION_SET;
        
        if (procName.equalsIgnoreCase("NewReservation")) {
            c_id = (Long)args[1];
            f_id = (Long)args[2];
        }
        else if (procName.equalsIgnoreCase("FindOpenSeats")) {
            f_id = (Long)args[0];
            readonly = this.catalogContext.getPartitionSetSingleton(base_partition);
        }
        else if (procName.equalsIgnoreCase("UpdateReservation")) {
            f_id = (Long)args[2];
        }
        else if (procName.equalsIgnoreCase("DeleteReservation")) {
            c_id = (Long)args[1];
            f_id = (Long)args[0];
            if (c_id == VoltType.NULL_BIGINT) {
                c_id = null;
            }
        }
        else if (procName.equalsIgnoreCase("UpdateCustomer")) {
            c_id = (Long)args[0];
        }
        
        // Construct partition information!
        if (f_id != null && c_id != null) {
            partitions = new PartitionSet();
            partitions.add(hasher.hash(f_id));
            partitions.add(hasher.hash(c_id));
        }
        else if (f_id != null) {
            partitions = this.catalogContext.getPartitionSetSingleton(hasher.hash(f_id));
        }
        else if (c_id != null) {
            partitions = this.catalogContext.getPartitionSetSingleton(hasher.hash(c_id));    
        }
        else {
            partitions = this.catalogContext.getPartitionSetSingleton(base_partition);
        }
        
        ret.createInitialEstimate(partitions, readonly, EMPTY_PARTITION_SET);
        return ((T)ret);
    }
}
