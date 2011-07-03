package edu.mit.hstore;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.Logger;

import edu.brown.hashing.AbstractHasher;
import edu.brown.utils.LoggerUtil;
import edu.brown.utils.LoggerUtil.LoggerBoolean;
import edu.mit.hstore.dtxn.LocalTransactionState;

public class NewOrderInspector {
    private static final Logger LOG = Logger.getLogger(NewOrderInspector.class);
    private final static LoggerBoolean debug = new LoggerBoolean(LOG.isDebugEnabled());
    private final static LoggerBoolean trace = new LoggerBoolean(LOG.isTraceEnabled());
    static {
        LoggerUtil.attachObserver(LOG, debug, trace);
    }
    
    private final HStoreSite hstore_site;
    private final HStoreConf hstore_conf;
    
    /**
     * W_ID String -> W_ID Short
     */
    private final Map<String, Short> neworder_hack_w_id;
    /**
     * W_ID Short -> PartitionId
     */
    private final Map<Short, Integer> neworder_hack_hashes;
    
    public NewOrderInspector(HStoreSite hstore_site) {
        this.hstore_site = hstore_site;
        this.hstore_conf = hstore_site.getHStoreConf();
        
        AbstractHasher hasher = hstore_site.getHasher();
        this.neworder_hack_hashes = new HashMap<Short, Integer>();
        this.neworder_hack_w_id = new HashMap<String, Short>();
        for (Short w_id = 0; w_id < 256; w_id++) {
            this.neworder_hack_w_id.put(w_id.toString(), w_id);
            this.neworder_hack_hashes.put(w_id, hasher.hash(w_id.intValue()));
        } // FOR
    }
    
    /**
     * 
     * @param ts
     * @param args
     * @return
     */
    public boolean initializeTransaction(LocalTransactionState ts, Object args[]) {
        assert(ts.getProcedureName().equalsIgnoreCase("neworder")) : "Unable to use NewOrder cheat for " + ts;
        Short w_id = this.neworder_hack_w_id.get(args[0].toString());
        assert(w_id != null);
        Integer w_id_partition = this.neworder_hack_hashes.get(w_id);
        assert(w_id_partition != null);
        short inner[] = (short[])args[5];
        
        boolean predict_singlePartition = true;
        Set<Integer> done_partitions = ts.getDonePartitions();
        if (hstore_conf.site.exec_neworder_cheat_done_partitions) done_partitions.addAll(this.hstore_site.getAllPartitionIds());
        
        short last_w_id = w_id.shortValue();
        Integer last_partition = w_id_partition;
        if (hstore_conf.site.exec_neworder_cheat_done_partitions) done_partitions.remove(w_id_partition);
        for (short s_w_id : inner) {
            if (s_w_id != last_w_id) {
                last_partition = this.neworder_hack_hashes.get(s_w_id);
                last_w_id = s_w_id;
            }
            if (w_id_partition.equals(last_partition) == false) {
                predict_singlePartition = false;
                if (hstore_conf.site.exec_neworder_cheat_done_partitions) {
                    done_partitions.remove(last_partition);
                } else break;
            }
        } // FOR
        if (trace.get()) LOG.trace(String.format("%s - SinglePartitioned=%s, W_ID=%d, S_W_IDS=%s", ts, predict_singlePartition, w_id, Arrays.toString(inner)));
        return (predict_singlePartition);
    }
}
