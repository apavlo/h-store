package org.voltdb.benchmark.tpcc;

import java.lang.reflect.Field;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.voltdb.CatalogContext;

import edu.brown.utils.StringUtil;

/**
 * TPC-C Benchmark Configuration
 * @author pavlo
 */
public final class TPCCConfig {

    public int first_warehouse = TPCCConstants.STARTING_WAREHOUSE;
    
    public int num_warehouses = 1;
    public boolean warehouse_per_partition = false;
    public boolean warehouse_affinity = false;
    public boolean warehouse_debug = false;
    public boolean warehouse_pairing = false;
    public boolean reset_on_clear = false;
    
    public int num_loadthreads = 1;
    public boolean loadthread_per_warehouse = false;
    
    public boolean noop = false;
    public boolean neworder_only = false;
    public boolean neworder_multip = true;
    public boolean neworder_skew_warehouse = false;
    /** Percentage of neworder txns that will abort */
    public int neworder_abort = TPCCConstants.NEWORDER_ABORT;
    /** Prevent all distributed neworder txns from being aborted*/
    public boolean neworder_abort_no_multip = false;
    /** Prevent all single-partition neworder txns from being aborted*/
    public boolean neworder_abort_no_singlep = false;
    
    /** Percentage of neworder txns that are forced to be multi-partitioned */
    public double neworder_multip_mix = -1;
    /** Whether neworder txns that are forced to be remote or not */
    public boolean neworder_multip_remote = false;
    
    public boolean payment_only = false;
    public boolean payment_multip = true;
    public boolean payment_multip_remote = false;
    /** Percentage of neworder txns that are forced to be multi-partitioned */
    public double payment_multip_mix = -1;

    /** If set to true, then we will use temporal skew for generating warehouse ids */
    public boolean temporal_skew = false;
    /** Percentage of warehouse ids that will be temporally skewed during the benchmark run */
    public int temporal_skew_mix = 0;
    public boolean temporal_skew_rotate = false;
    
    /**
     * Scale the number of items based on the client scalefactor.
     * @see HStoreConf.ClientConf.scalefactor
     */
    public boolean scale_items = false;
    
    private TPCCConfig() {
        // Nothing
    }
    private TPCCConfig(CatalogContext catalogContext, Map<String, String> params) {
        for (Entry<String, String> e : params.entrySet()) {
            String key = e.getKey();
            String val = e.getValue();

            // FIRST WAREHOUSE ID
            if (key.equalsIgnoreCase("first_warehouse") && !val.isEmpty()) {
                first_warehouse = Integer.parseInt(val);
            }
            // NUMBER OF WAREHOUSES
            else if (key.equalsIgnoreCase("warehouses") && !val.isEmpty()) {
                num_warehouses = Integer.parseInt(val);
            }
            // ONE WAREHOUSE PER PARTITION
            else if (key.equalsIgnoreCase("warehouse_per_partition") && !val.isEmpty()) {
                warehouse_per_partition = Boolean.parseBoolean(val);
            }
            // WAREHOUSE AFFINITY
            else if (key.equalsIgnoreCase("warehouse_affinity") && !val.isEmpty()) {
                warehouse_affinity = Boolean.parseBoolean(val);
            }
            // WAREHOUSE PAIRING
            else if (key.equalsIgnoreCase("warehouse_pairing") && !val.isEmpty()) {
                warehouse_pairing = Boolean.parseBoolean(val);
            }
            // ENABLE WAREHOUSE DEBUGGING
            else if (key.equalsIgnoreCase("warehouse_debug") && !val.isEmpty()) {
                warehouse_debug = Boolean.parseBoolean(val);
            }
            // RESET WHEN CLEAR IS CALLED FROM BENCHMARKCONTROLLER
            else if (key.equalsIgnoreCase("reset_on_clear") && !val.isEmpty()) {
                reset_on_clear = Boolean.parseBoolean(val);
            }
            // LOAD THREADS
            else if (key.equalsIgnoreCase("loadthreads") && !val.isEmpty()) {
                num_loadthreads = Integer.parseInt(val);
            }
            // ONE LOADTHREAD PER WAREHOUSE
            else if (key.equalsIgnoreCase("loadthread_per_warehouse") && !val.isEmpty()) {
                loadthread_per_warehouse = Boolean.parseBoolean(val);
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
                neworder_abort = Integer.parseInt(val);
            }
            // PREVENT ABORTS FOR NEWORDER DTXNS
            else if (key.equalsIgnoreCase("neworder_abort_no_multip") && !val.isEmpty()) {
                neworder_abort_no_multip = Boolean.parseBoolean(val);
            }
            // PREVENT ABORTS FOR SINGLE-PARTITION NEWORDER TXNS
            else if (key.equalsIgnoreCase("neworder_abort_np_singlep") && !val.isEmpty()) {
                neworder_abort_no_singlep = Boolean.parseBoolean(val);
            }
            // NEWORDER DTXN PERCENTAGE
            else if (key.equalsIgnoreCase("neworder_multip") && !val.isEmpty()) {
                neworder_multip = Boolean.parseBoolean(val);
            }
            // FORCE NEWORDER REMOTE W_ID DTXNS
            else if (key.equalsIgnoreCase("neworder_multip_remote") && !val.isEmpty()) {
                neworder_multip_remote = Boolean.parseBoolean(val);
            }
            // % OF MULTI-PARTITION NEWORDERS
            else if (key.equalsIgnoreCase("neworder_multip_mix") && !val.isEmpty()) {
                neworder_multip_mix = Double.parseDouble(val);
            }
            // SKEW NEWORDERS W_IDS
            else if (key.equalsIgnoreCase("neworder_skew_warehouse") && !val.isEmpty()) {
                neworder_skew_warehouse = Boolean.parseBoolean(val);
            }
            
            // ONLY PAYMENT
            else if (key.equalsIgnoreCase("payment_only") && !val.isEmpty()) {
                payment_only = Boolean.parseBoolean(val);
            }
            // ALLOW PAYMENT DTXNS
            else if (key.equalsIgnoreCase("payment_multip") && !val.isEmpty()) {
                payment_multip = Boolean.parseBoolean(val);
            }
            // FORCE PAYMENT REMOTE W_ID DTXNS
            else if (key.equalsIgnoreCase("payment_multip_remote") && !val.isEmpty()) {
                payment_multip_remote = Boolean.parseBoolean(val);
            }
            // PAYMENT DTXN PERCENTAGE
            else if (key.equalsIgnoreCase("payment_multip_mix") && !val.isEmpty()) {
                payment_multip_mix = Double.parseDouble(val);
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
            
            // # OF ITEMS
            else if (key.equalsIgnoreCase("scale_items") && !val.isEmpty()) {
                scale_items = Boolean.parseBoolean(val);
            }
        } // FOR
        
        if (warehouse_per_partition) num_warehouses = catalogContext.numberOfPartitions;
        if (loadthread_per_warehouse) {
            num_loadthreads = num_warehouses;
        } else {
            num_loadthreads = Math.min(num_warehouses, num_loadthreads);
        }
    }
    
    public static TPCCConfig defaultConfig() {
        return new TPCCConfig();
    }
    
    public static TPCCConfig createConfig(CatalogContext catalogContext, Map<String, String> params) {
        return new TPCCConfig(catalogContext, params);
    }
    
    public void disableDistributedTransactions() {
        this.neworder_multip = false;
        this.neworder_multip_mix = 0;
        this.payment_multip = false;
        this.payment_multip_mix = 0;
    }
    
    @Override
    public String toString() {
        return StringUtil.formatMaps(this.debugMap());
    }
    
    public Map<String, Object> debugMap() {
        Class<?> confClass = this.getClass();
        Map<String, Object> m = new LinkedHashMap<String, Object>();
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
