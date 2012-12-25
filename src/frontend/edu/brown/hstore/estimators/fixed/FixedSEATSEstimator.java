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
    private static final LoggerBoolean debug = new LoggerBoolean(LOG.isDebugEnabled());
    private static final LoggerBoolean trace = new LoggerBoolean(LOG.isTraceEnabled());
    static {
        LoggerUtil.attachObserver(LOG, debug, trace);
    }
    
    /**
     * Constructor
     * @param hstore_site
     */
    public FixedSEATSEstimator(PartitionEstimator p_estimator) {
        super(p_estimator);
    }
    
    @Override
    public <T extends EstimatorState> T startTransactionImpl(Long txn_id, int base_partition, Procedure catalog_proc, Object[] args) {
        String procName = catalog_proc.getName();
        long f_id = VoltType.NULL_BIGINT;
        long c_id = VoltType.NULL_BIGINT;
        PartitionSet ret = null;
        
        if (procName.equalsIgnoreCase("NewReservation") ||
            procName.equalsIgnoreCase("UpdateReservation")) {
            c_id = (Long)args[1];
            f_id = (Long)args[2];
        }
        else if (procName.equalsIgnoreCase("DeleteReservation")) {
            c_id = (Long)args[1];
            f_id = (Long)args[0];
        }
        else if (procName.equalsIgnoreCase("FindOpenSeats")) {
            f_id = (Long)args[0];
        }
        else if (procName.equalsIgnoreCase("UpdateCustomer")) {
            c_id = (Long)args[0];
        }
        
        // Construct partitions collection!
        if (f_id != VoltType.NULL_BIGINT && c_id != VoltType.NULL_BIGINT) {
            ret = new PartitionSet();
            ret.add(hasher.hash(f_id));
            ret.add(hasher.hash(c_id));
        }
        else if (f_id != VoltType.NULL_BIGINT) {
            ret = this.singlePartitionSets.get(hasher.hash(f_id));
        }
        else if (c_id != VoltType.NULL_BIGINT) {
            ret = this.singlePartitionSets.get(hasher.hash(c_id));    
        }
        else {
            ret = this.catalogContext.getAllPartitionIds();
        }
        
        return null;
    }
}
