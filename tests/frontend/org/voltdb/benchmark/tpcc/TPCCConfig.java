package org.voltdb.benchmark.tpcc;

import java.lang.reflect.Field;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.collections15.map.ListOrderedMap;

import edu.brown.utils.StringUtil;

public final class TPCCConfig {

    public int firstWarehouse = TPCCConstants.STARTING_WAREHOUSE;
    
    public int loadthreads = 1;
    public boolean warehouse_affinity = false;
    public boolean noop = false;
    public boolean neworder_only = false;
    public boolean neworder_abort = false;
    public boolean neworder_multip = false;
    public boolean neworder_skew_warehouse = false;

    /** Percentage of neworder txns that are forced to be multi-partitioned */
    public int neworder_multip_mix = 0;
    
    /** If set to true, then we will use temporal skew for generating warehouse ids */
    public boolean temporal_skew = false;
    /** Percentage of warehouse ids that will be temporally skewed during the benchmark run */
    public int temporal_skew_mix = 0;
    public boolean temporal_skew_rotate = false;
    
    private TPCCConfig() {
        // Nothing
    }
    private TPCCConfig(Map<String, String> params) {
        for (Entry<String, String> e : params.entrySet()) {
            String key = e.getKey();
            String val = e.getValue();
            
            // FIRST WAREHOUSE ID
            if (key.equalsIgnoreCase("first_warehouse") && !val.isEmpty()) {
                firstWarehouse = Integer.parseInt(val);
            }
            // LOAD THREADS
            else if (key.equalsIgnoreCase("loadthreads") && !val.isEmpty()) {
                loadthreads = Integer.parseInt(val);
            }
            // WAREHOUSE AFFINITY
            else if (key.equalsIgnoreCase("warehouse_affinity") && !val.isEmpty()) {
                warehouse_affinity = Boolean.parseBoolean(val);
            }
            // NOOPs
            else if (key.equalsIgnoreCase("noop") && !val.isEmpty()) {
                noop = Boolean.parseBoolean(val);
            }
            // ONLY NEW ORDER
            else if (key.equalsIgnoreCase("neworder_only") && !val.isEmpty()) {
                neworder_only = Boolean.parseBoolean(val);
            }
            // ALLOW NEWORDER ABORTS
            else if (key.equalsIgnoreCase("neworder_abort") && !val.isEmpty()) {
                neworder_abort = Boolean.parseBoolean(val);
            }
            // ALLOW NEWORDER DTXNS
            else if (key.equalsIgnoreCase("neworder_multip") && !val.isEmpty()) {
                neworder_multip = Boolean.parseBoolean(val);
            }
            // % OF MULTI-PARTITION NEWORDERS
            else if (key.equalsIgnoreCase("neworder_multip_mix") && !val.isEmpty()) {
                neworder_multip_mix = Integer.parseInt(val);
            }
            // SKEW NEWORDERS W_IDS
            else if (key.equalsIgnoreCase("neworder_skew_warehouse") && !val.isEmpty()) {
                neworder_skew_warehouse = Boolean.parseBoolean(val);
            }
            // TEMPORAL SKEW
            else if (key.equalsIgnoreCase("temporal_skew") && !val.isEmpty()) {
                temporal_skew = Boolean.parseBoolean(val);
            }
            // TEMPORAL SKEW MIX
            else if (key.equalsIgnoreCase("temporal_skew_mix") && !val.isEmpty()) {
                temporal_skew_mix = Integer.parseInt(val);
            }
            // TEMPORAL SKEW ROTATE
            else if (key.equalsIgnoreCase("temporal_skew_rotate") && !val.isEmpty()) {
                temporal_skew_rotate = Boolean.parseBoolean(val);
            }
        } // FOR
    }
    
    public static TPCCConfig defaultConfig() {
        return new TPCCConfig();
    }
    
    public static TPCCConfig createConfig(Map<String, String> params) {
        return new TPCCConfig(params);
    }
    
    @Override
    public String toString() {
        return StringUtil.formatMaps(this.debugMap());
    }
    
    public Map<String, Object> debugMap() {
        Class<?> confClass = this.getClass();
        Map<String, Object> m = new ListOrderedMap<String, Object>();
        for (Field f : confClass.getFields()) {
            Object obj = null;
            try {
                obj = f.get(this);
            } catch (IllegalAccessException ex) {
                throw new RuntimeException(ex);
            }
            m.put(f.getName().toUpperCase(), obj);
        } // FOR
        return (m);
    }
}
