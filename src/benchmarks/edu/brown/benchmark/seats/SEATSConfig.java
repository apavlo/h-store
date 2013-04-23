package edu.brown.benchmark.seats;

import java.lang.reflect.Field;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.collections15.map.ListOrderedMap;
import org.voltdb.CatalogContext;

import edu.brown.utils.StringUtil;

public final class SEATSConfig {
    
    public boolean force_all_singlepartition = false;
    public boolean force_all_distributed = false;

    private SEATSConfig(CatalogContext catalogContext, Map<String, String> params) {
        for (Entry<String, String> e : params.entrySet()) {
            String key = e.getKey();
            String val = e.getValue();
            
            // FORCE ALL SINGLE-PARTITIONED TXNS
            if (key.equalsIgnoreCase("force_all_singlepartition") && !val.isEmpty()) {
                this.force_all_singlepartition = Boolean.parseBoolean(val);
            }
            // FORCE ALL DISTRIBUTED TXNS
            else if (key.equalsIgnoreCase("force_all_distributed") && !val.isEmpty()) {
                this.force_all_distributed = Boolean.parseBoolean(val);
            }
        } // FOR
    }
    
    public static SEATSConfig createConfig(CatalogContext catalogContext, Map<String, String> params) {
        return new SEATSConfig(catalogContext, params);
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
