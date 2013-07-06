package edu.brown.hstore.estimators.fixed;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.log4j.Logger;
import org.voltdb.catalog.Procedure;
import org.voltdb.catalog.Statement;

import edu.brown.catalog.special.CountedStatement;
import edu.brown.hstore.estimators.EstimatorState;
import edu.brown.logging.LoggerUtil;
import edu.brown.logging.LoggerUtil.LoggerBoolean;
import edu.brown.utils.PartitionEstimator;
import edu.brown.utils.PartitionSet;

/**
 * TPC-C Benchmark Fixed Estimator
 * @author pavlo
 */
public class FixedTPCCEstimator extends AbstractFixedEstimator {
    private static final Logger LOG = Logger.getLogger(FixedTPCCEstimator.class);
    private static final LoggerBoolean debug = new LoggerBoolean();
    private static final LoggerBoolean trace = new LoggerBoolean();
    static {
        LoggerUtil.attachObserver(LOG, debug, trace);
    }
    
    /**
     * W_ID Short -> PartitionId
     */
    private int[] neworder_hack_hashes;
    
    /**
     * NewOrder Prefetchable Query Information
     */
    private final Statement[] neworder_prefetchables;
    private final List<CountedStatement[]> neworder_countedstmts = new ArrayList<CountedStatement[]>();
    
    /**
     * Constructor
     * @param hstore_site
     */
    public FixedTPCCEstimator(PartitionEstimator p_estimator) {
        super(p_estimator);
        
        // Prefetchable Statements
        if (hstore_conf.site.exec_prefetch_queries) {
            Procedure catalog_proc = catalogContext.procedures.getIgnoreCase("neworder");
            String prefetchables[] = { "getStockInfo" };
            this.neworder_prefetchables = new Statement[prefetchables.length];
            for (int i = 0; i < this.neworder_prefetchables.length; i++) {
                Statement catalog_stmt = catalog_proc.getStatements().getIgnoreCase(prefetchables[i]);
                assert(catalog_stmt != null) :
                    String.format("Invalid prefetchable Statement %s.%s",
                                  catalog_proc.getName(), prefetchables[i]);
                this.neworder_prefetchables[i] = catalog_stmt;
            } // FOR
        } else {
            this.neworder_prefetchables = null;    
        }
        for (int i = 0; i < 20; i++) {
            this.neworder_countedstmts.add(null);
        } // FOR
    }
    
    private int getPartition(short w_id) {
        if (this.neworder_hack_hashes == null || this.neworder_hack_hashes.length <= w_id) {
            synchronized (this) {
                if (this.neworder_hack_hashes == null || this.neworder_hack_hashes.length <= w_id) {
                    int temp[] = new int[w_id+1];
                    for (int i = 0; i < temp.length; i++) {
                        temp[i] = this.hasher.hash(i);
                    } // FOR
                    this.neworder_hack_hashes = temp;
                }
            } // SYNCH
        }
        return (this.neworder_hack_hashes[w_id]);
    }
    
    @SuppressWarnings("unchecked")
    @Override
    public <T extends EstimatorState> T startTransactionImpl(Long txn_id, int base_partition, Procedure catalog_proc, Object[] args) {
        String procName = catalog_proc.getName();
        FixedEstimatorState ret = new FixedEstimatorState(this.catalogContext, txn_id, base_partition);
        
        PartitionSet partitions = null;
        PartitionSet readonly = null;
        if (procName.equalsIgnoreCase("neworder")) {
            partitions = this.newOrder(ret, args);
            readonly = EMPTY_PARTITION_SET;
        }
        else if (procName.startsWith("payment")) {
            int hash_w_id = this.getPartition((Short)args[0]);
            int hash_c_w_id = this.getPartition((Short)args[3]);
            if (hash_w_id == hash_c_w_id) {
                partitions = this.catalogContext.getPartitionSetSingleton(hash_w_id);
            } else {
                partitions = new PartitionSet();
                partitions.add(hash_w_id);
                partitions.add(hash_c_w_id);
            }
            readonly = EMPTY_PARTITION_SET;
        }
        else if (procName.equalsIgnoreCase("delivery")) {
            partitions = this.catalogContext.getPartitionSetSingleton(base_partition);
            readonly = EMPTY_PARTITION_SET;
        }
        else {
            partitions = readonly = this.catalogContext.getPartitionSetSingleton(base_partition);
        }
        assert(partitions != null);
        assert(readonly != null);
        
        ret.createInitialEstimate(partitions, readonly, EMPTY_PARTITION_SET);
        return ((T)ret);
    }
    
    private PartitionSet newOrder(FixedEstimatorState state, Object args[]) {
        final Short w_id = (Short)args[0];
        assert(w_id != null);
        short s_w_ids[] = (short[])args[5];
        
        int base_partition = this.getPartition(w_id.shortValue());
        PartitionSet touchedPartitions = this.catalogContext.getPartitionSetSingleton(base_partition);
        assert(touchedPartitions != null) : "base_partition = " + base_partition;
        for (int i = 0; i < s_w_ids.length; i++) {
            if (s_w_ids[i] != w_id) {
                if (this.neworder_prefetchables != null) {
                    for (CountedStatement cntStmt : this.getNewOrderPrefetchables(i)) {
                        state.addPrefetchableStatement(cntStmt);
                    } // FOR
                }
                if (touchedPartitions.size() == 1) {
                    touchedPartitions = new PartitionSet(base_partition);
                }
                touchedPartitions.add(this.getPartition(s_w_ids[i]));
            }
        } // FOR
        if (debug.val) 
            LOG.debug(String.format("NewOrder - [W_ID=%d / S_W_IDS=%s] => [BasePartition=%s / Partitions=%s]",
                      w_id, Arrays.toString(s_w_ids), 
                      base_partition, touchedPartitions));
        return (touchedPartitions);        
    }
    
    private CountedStatement[] getNewOrderPrefetchables(int index) {
        CountedStatement[] ret = this.neworder_countedstmts.get(index);
        if (ret == null) {
            // There's a race condition here, but who cares...
            ret = new CountedStatement[this.neworder_prefetchables.length];
            for (int i = 0; i < ret.length; i++) {
                ret[i] = new CountedStatement(this.neworder_prefetchables[i], i);
            } // FOR
            this.neworder_countedstmts.set(index, ret);
        }
        return (ret);
    }
}
