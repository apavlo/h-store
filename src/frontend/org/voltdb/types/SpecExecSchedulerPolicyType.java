package org.voltdb.types;

import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;

/**
 * This defines how the SpecExecScheduler will choose the next
 * transaction to speculatively execute.
 * @author xin
 * @author pavlo
 */
public enum SpecExecSchedulerPolicyType {
    /**
     * Pick the first matching candidate that we find.
     * This is the default configuration.
     */
    FIRST,
    /**
     * Pick the last matching candidate that we find.
     * This is only useful for testing and experiments.
     */
    LAST,
    /**
     * Pick the candidate with the shortest estimated execution time.
     * This requires using a transaction estimator that supports run time calculations
     */
    SHORTEST,
    /**
     * Pick the candidate with the longest estimated execution time.
     * This requires using a transaction estimator that supports run time calculations
     */
    LONGEST;
      
    private static final Map<String, SpecExecSchedulerPolicyType> name_lookup = new HashMap<String, SpecExecSchedulerPolicyType>();
    static {
        for (SpecExecSchedulerPolicyType e : EnumSet.allOf(SpecExecSchedulerPolicyType.class)) {
            SpecExecSchedulerPolicyType.name_lookup.put(e.name().toLowerCase(), e);
        } // FOR
    } // STATIC
      
    public static SpecExecSchedulerPolicyType get(String name) {
        return SpecExecSchedulerPolicyType.name_lookup.get(name.toLowerCase());
    }
} // ENUM