package org.voltdb.types;

import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;

/**
 * AntiCacheDB Types
 */
public enum AntiCacheDBType {
    /**
     * BerkeleyDB disk-backed store
     */
    BERKELEY,
    /**
     * NVM backed store (generic)
     */
    NVM
    ;

    private static final Map<String, AntiCacheDBType> name_lookup = new HashMap<String, AntiCacheDBType>();
    static {
        for (AntiCacheDBType vt : EnumSet.allOf(AntiCacheDBType.class)) {
            name_lookup.put(vt.name().toLowerCase(), vt);
        }
    } // Static

    public static AntiCacheDBType get(int idx) {
        AntiCacheDBType values[] = AntiCacheDBType.values();
        if (idx < 0 || idx >= values.length) {
            return (null);
        }
        return (values[idx]);
    }

    public static AntiCacheDBType get(String name) {
        return AntiCacheDBType.name_lookup.get(name.toLowerCase());
    }
}
