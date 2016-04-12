package org.voltdb.types;

import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;

/**
 * Speculative Execution Stall Point Types
 * This corresponds to where in the distributed txn that the executor is stalling.
 */
public enum SpeculationType {
    
    /**
     * Invalid/null stall point
     */
    NULL,
    /**
     * This is when the partition executor is allowed to jump ahead in time and execute txns
     * assuming that nobody is going to show up with a lower timestamp.
     */
    SP1_IDLE,
    /**
     * This stall point occurs on the local partition where the dtxn has acquired the lock
     * for it's local partition but it's waiting to hear back about getting the locks
     * from the other remote partitions.
     */
    SP1_LOCAL,
    /**
     * This stall point occurs on the base partition when the
     * transaction is waiting for a WorkResponse from a remote partition.
     */
    SP2_LOCAL,
    /**
     * This stall point occurs on the remote partition while the executor
     * is waiting <b>before</b> the first query request for the distributed transaction
     */
    SP3_REMOTE_BEFORE,
    /**
     * This stall point occurs on the remote partition while the executor
     * is waiting <b>after</b> the first query request for the distributed transaction
     */
    SP3_REMOTE_AFTER,
    /**
     * This stall point occurs on the base partition while the executor is 
     * waiting for the 2PC-PREPARE responses from all of the dtxn's remote partitions.
     */
    SP4_LOCAL,
    /**
     * This stall point occurs on the remote partition while the executor is 
     * waiting for the 2PC-PREPARE acknowledgment from the dtxn's base partition.
     */
    SP4_REMOTE,
    ;

    private static final Map<String, SpeculationType> name_lookup = new HashMap<String, SpeculationType>();
    static {
        for (SpeculationType vt : EnumSet.allOf(SpeculationType.class)) {
            SpeculationType.name_lookup.put(vt.name().toLowerCase(), vt);
        }
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
