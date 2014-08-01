package org.voltdb.types;

import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;

public enum AntiCacheEvictionPolicyType {
    EVEN,
    PROPORTIONAL,
    UNEVICTION_RATIO,
    ACCESS_RATE;
    
    private static final Map<Integer, AntiCacheEvictionPolicyType> idx_lookup = new HashMap<Integer, AntiCacheEvictionPolicyType>();
    private static final Map<String, AntiCacheEvictionPolicyType> name_lookup = new HashMap<String, AntiCacheEvictionPolicyType>();
    static {
        for (AntiCacheEvictionPolicyType vt : EnumSet.allOf(AntiCacheEvictionPolicyType.class)) {
            AntiCacheEvictionPolicyType.idx_lookup.put(vt.ordinal(), vt);
            AntiCacheEvictionPolicyType.name_lookup.put(vt.name().toLowerCase(), vt);
        }
    }
    
    public static AntiCacheEvictionPolicyType get(String name) {
        AntiCacheEvictionPolicyType ret = AntiCacheEvictionPolicyType.name_lookup.get(name.toLowerCase());
        return (ret);
    }
}
