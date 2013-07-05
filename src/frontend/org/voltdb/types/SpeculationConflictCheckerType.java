package org.voltdb.types;

import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;

/**
 * Speculative Execution Conflict Checking Types
 * @author pavlo
 */
public enum SpeculationConflictCheckerType {
    /**
     * Table-level Conflict Detection
     */
    TABLE,
    /**
     * Markov-model Row-level Conflict Detection
     * Use the row-based MarkovConflictChecker to determine whether queued transactions
     * conflict with the current distributed transaction. This is used to select
     * speculative execution candidates at runtime.
     */
    MARKOV,
    /**
     * Unsafe! Let anything go!
     * All the PartitionExecutor to speculatively <b>any</b> transaction whenever a
     * distributed transaction is stalled. This is a bad thing to do because it will
     * not check whether there are any conflicts or consistency violations. You most
     * likely do not want to enable this option unless you are Andy and its right
     * before you have to go on a speaking tour and need to show comparison
     * numbers in your job talk.
     */
    UNSAFE,
    /**
     * Optimistic Concurrency Control
     */
    OPTIMISTIC
    ;

    private static final Map<String, SpeculationConflictCheckerType> name_lookup = new HashMap<String, SpeculationConflictCheckerType>();
    static {
        for (SpeculationConflictCheckerType vt : EnumSet.allOf(SpeculationConflictCheckerType.class)) {
            name_lookup.put(vt.name().toLowerCase(), vt);
        }
    } // STATIC

    public static SpeculationConflictCheckerType get(int idx) {
        SpeculationConflictCheckerType values[] = SpeculationConflictCheckerType.values();
        if (idx < 0 || idx >= values.length) {
            return (null);
        }
        return (values[idx]);
    }

    public static SpeculationConflictCheckerType get(String name) {
        return SpeculationConflictCheckerType.name_lookup.get(name.toLowerCase());
    }

}
