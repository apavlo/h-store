package edu.brown.hstore.estimators;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import org.apache.log4j.Logger;
import org.voltdb.CatalogContext;
import org.voltdb.catalog.Procedure;

import edu.brown.hashing.AbstractHasher;
import edu.brown.logging.LoggerUtil;
import edu.brown.logging.LoggerUtil.LoggerBoolean;
import edu.brown.utils.ParameterMangler;
import edu.brown.utils.PartitionSet;

public class TPCCEstimator extends AbstractEstimator {
    private static final Logger LOG = Logger.getLogger(TPCCEstimator.class);
    private static final LoggerBoolean debug = new LoggerBoolean(LOG.isDebugEnabled());
    private static final LoggerBoolean trace = new LoggerBoolean(LOG.isTraceEnabled());
    static {
        LoggerUtil.attachObserver(LOG, debug, trace);
    }
    
    /**
     * W_ID Short -> PartitionId
     */
    private final Map<Short, Integer> neworder_hack_hashes = new HashMap<Short, Integer>();
    
    /**
     * Constructor
     * @param hstore_site
     */
    public TPCCEstimator(CatalogContext catalogContext, Map<Procedure, ParameterMangler> manglers, AbstractHasher hasher) {
        super(catalogContext, manglers, hasher);
    }
    
    private Integer getPartition(Short w_id) {
        Integer partition = this.neworder_hack_hashes.get(w_id);
        if (partition == null) {
            partition = this.hasher.hash(w_id);
            this.neworder_hack_hashes.put(w_id, partition);
        }
        assert(partition != null);
        return (partition);
    }
    
    @Override
    protected PartitionSet initializeTransactionImpl(Procedure catalog_proc, Object args[], Object mangled[]) {
        String procName = catalog_proc.getName();
        PartitionSet ret = null;
        
        if (procName.equalsIgnoreCase("neworder")) {
            ret = this.newOrder(args, mangled);
        } else if (procName.startsWith("payment")) {
            Integer hash_w_id = this.getPartition((Short)mangled[0]);
            Integer hash_c_w_id = this.getPartition((Short)mangled[3]);
            if (hash_w_id.equals(hash_c_w_id)) {
                ret = this.singlePartitionSets.get(hash_w_id);
            } else {
                ret = new PartitionSet();
                ret.add(hash_w_id);
                ret.add(hash_c_w_id);
            }
        }
        return (ret);
    }
    
    private PartitionSet newOrder(Object args[], Object mangled[]) {
        final Short w_id = (Short)mangled[0];
        assert(w_id != null);
        short s_w_ids[] = (short[])args[5];
        
        Integer base_partition = this.getPartition(w_id);
        PartitionSet touchedPartitions = this.singlePartitionSets.get(base_partition);
        assert(touchedPartitions != null) : "base_partition = " + base_partition;
        for (short s_w_id : s_w_ids) {
            if (s_w_id != w_id) {
                if (touchedPartitions.size() == 1) {
                    touchedPartitions = new PartitionSet(touchedPartitions);
                }
                touchedPartitions.add(this.getPartition(s_w_id));
            }
        } // FOR
        if (debug.get())
            LOG.debug(String.format("NewOrder - Partitions=%s, W_ID=%d, S_W_IDS=%s",
                                    touchedPartitions, w_id, Arrays.toString(s_w_ids)));
        return (touchedPartitions);        
    }
}
