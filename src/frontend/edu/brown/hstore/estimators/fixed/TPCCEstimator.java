package edu.brown.hstore.estimators.fixed;

import java.util.Arrays;

import org.apache.log4j.Logger;
import org.voltdb.catalog.Procedure;
import org.voltdb.catalog.Statement;

import edu.brown.hstore.Hstoreservice.Status;
import edu.brown.hstore.estimators.EstimatorState;
import edu.brown.hstore.estimators.Estimate;
import edu.brown.logging.LoggerUtil;
import edu.brown.logging.LoggerUtil.LoggerBoolean;
import edu.brown.utils.PartitionEstimator;
import edu.brown.utils.PartitionSet;

public class TPCCEstimator extends FixedEstimator {
    private static final Logger LOG = Logger.getLogger(TPCCEstimator.class);
    private static final LoggerBoolean debug = new LoggerBoolean(LOG.isDebugEnabled());
    private static final LoggerBoolean trace = new LoggerBoolean(LOG.isTraceEnabled());
    static {
        LoggerUtil.attachObserver(LOG, debug, trace);
    }
    
    /**
     * W_ID Short -> PartitionId
     */
    private Integer[] neworder_hack_hashes;
    
    /**
     * Constructor
     * @param hstore_site
     */
    public TPCCEstimator(PartitionEstimator p_estimator) {
        super(p_estimator);
    }
    
    private Integer getPartition(short w_id) {
        if (this.neworder_hack_hashes == null || this.neworder_hack_hashes.length <= w_id) {
            synchronized (this) {
                if (this.neworder_hack_hashes == null || this.neworder_hack_hashes.length <= w_id) {
                    this.neworder_hack_hashes = new Integer[w_id+1];
                    for (int i = 0; i < this.neworder_hack_hashes.length; i++) {
                        this.neworder_hack_hashes[i] = this.hasher.hash(i);
                    } // FOR
                }
            } // SYNCH
        }
        return (this.neworder_hack_hashes[w_id]);
    }
    
    @SuppressWarnings("unchecked")
    @Override
    public EstimatorState startTransactionImpl(Long txn_id, int base_partition, Procedure catalog_proc, Object[] args) {
        String procName = catalog_proc.getName();
        FixedEstimatorState ret = new FixedEstimatorState(this.catalogContext, txn_id, base_partition);
        
        PartitionSet partitions = null;
        PartitionSet readonly = null;
        if (procName.equalsIgnoreCase("neworder")) {
            partitions = this.newOrder(args);
            readonly = EMPTY_PARTITION_SET;
        }
        else if (procName.startsWith("payment")) {
            Integer hash_w_id = this.getPartition((Short)args[0]);
            Integer hash_c_w_id = this.getPartition((Short)args[3]);
            if (hash_w_id.equals(hash_c_w_id)) {
                partitions = this.singlePartitionSets.get(hash_w_id);
            } else {
                partitions = new PartitionSet();
                partitions.add(hash_w_id);
                partitions.add(hash_c_w_id);
            }
            readonly = EMPTY_PARTITION_SET;
        }
        else if (procName.equalsIgnoreCase("delivery")) {
            partitions = catalogContext.getPartitionSetSingleton(base_partition);
            readonly = EMPTY_PARTITION_SET;
        }
        else {
            partitions = readonly = catalogContext.getPartitionSetSingleton(base_partition);
        }
        assert(partitions != null);
        assert(readonly != null);
        
        ret.createInitialEstimate(partitions, readonly, EMPTY_PARTITION_SET);
        return (ret);
    }
    
    @SuppressWarnings("unchecked")
    @Override
    public Estimate executeQueries(EstimatorState state, Statement[] catalog_stmts, PartitionSet[] partitions) {
        return state.getInitialEstimate();
    }

    @Override
    protected void completeTransaction(EstimatorState state, Status status) {
        // Nothing to do
    }
    
    private PartitionSet newOrder(Object args[]) {
        final Short w_id = (Short)args[0];
        assert(w_id != null);
        short s_w_ids[] = (short[])args[5];
        
        Integer base_partition = this.getPartition(w_id.shortValue());
        PartitionSet touchedPartitions = this.singlePartitionSets.get(base_partition);
        assert(touchedPartitions != null) : "base_partition = " + base_partition;
        for (short s_w_id : s_w_ids) {
            if (s_w_id != w_id) {
                if (touchedPartitions.size() == 1) {
                    touchedPartitions = new PartitionSet(base_partition);
                }
                touchedPartitions.add(this.getPartition(s_w_id));
            }
        } // FOR
        if (debug.get()) 
            LOG.debug(String.format("NewOrder - [W_ID=%d / S_W_IDS=%s] => [BasePartition=%s / Partitions=%s]",
                      w_id, Arrays.toString(s_w_ids), 
                      base_partition, touchedPartitions));
        return (touchedPartitions);        
    }

    @Override
    public void updateLogging() {
        // TODO Auto-generated method stub
        
    }
}
