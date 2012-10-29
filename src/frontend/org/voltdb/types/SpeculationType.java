package org.voltdb.types;

import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;

/**
 * Speculative Execution Stall Point Types
 */
public enum SpeculationType {
    
    /**
     * Invalid/null Stall Point
     */
    NULL,
    /**
     * This stall point occurs on the base partition when the
     * transaction is waiting for a WorkResponse from a remote partition.
     */
    SP1_LOCAL,
    /**
     * This stall point occurs on the base partition while the executor is 
     * waiting for the 2PC-PREPARE responses from all of the dtxn's remote partitions.
     */
    SP3_LOCAL,
    /**
     * This stall point occurs on the remote partition while the executor
     * is waiting for either a WorkRequest to process or a 2PC message
     */
    SP2_REMOTE,
    /**
     * This stall point occurs on the remote partition while the executor is 
     * waiting for the 2PC-PREPARE acknowledgment from the dtxn's base partition.
     */
    SP3_REMOTE;

    protected static final Map<String, SpeculationType> name_lookup = new HashMap<String, SpeculationType>();
    static {
        for (SpeculationType vt : EnumSet.allOf(SpeculationType.class)) {
            SpeculationType.name_lookup.put(vt.name().toLowerCase(), vt);
        }
    }

    public static Map<String, SpeculationType> getNameMap() {
        return name_lookup;
    }

    public static SpeculationType get(int idx) {
        SpeculationType values[] = SpeculationType.values();
        if (idx < 0 || idx >= values.length) {
            return (SpeculationType.NULL);
        }
        return (values[idx]);
    }

    public static SpeculationType get(String name) {
        SpeculationType ret = SpeculationType.name_lookup.get(name.toLowerCase());
        return (ret == null ? SpeculationType.NULL : ret);
    }

}
