package edu.brown.catalog;

import org.voltdb.catalog.CatalogType;

/**
 * Special utility class for PlanFragment ids
 * @author pavlo
 */
public abstract class PlanFragmentIdGenerator {
    
    private static final int READONLY_OFFSET = 16;
    private static final int FASTAGGREGATE_OFFSET = 17;
    private static final int FASTCOMBINE_OFFSET = 18;

    /**
     * @param next_id
     * @param readonly
     * @return
     */
    public static int createPlanFragmentId(int next_id,
                                             boolean readonly,
                                             boolean fastAggregate,
                                             boolean fastCombine) {
        // Read-Only
        if (readonly) {
            next_id |= 1 << READONLY_OFFSET;
        }
        // Fast Aggregate
        if (fastAggregate) {
            next_id |= 1 << FASTAGGREGATE_OFFSET;
        }
        // Fast Combine
        if (fastCombine) {
            next_id |= 1 << FASTCOMBINE_OFFSET;
        }
        
        return (next_id);
    }
    
    /**
     * Returns true if the PlanFragmnt id is marked as a read only
     * @param id
     * @return
     */
    public static boolean isPlanFragmentReadOnly(long id) {
        return ((id >>> READONLY_OFFSET & 1) == 1);
    }
    
    /**
     * Returns true if the PlanFragmnt id is marked as a fast aggregate
     * @param id
     * @return
     */
    public static boolean isPlanFragmentFastAggregate(long id) {
        return ((id >>> FASTAGGREGATE_OFFSET & 1) == 1);   
    }
    
    /**
     * 
     * @param id
     * @return
     */
    public static boolean isPlanFragmentFastCombine(long id) {
        return ((id >>> FASTCOMBINE_OFFSET & 1) == 1);
    }
    
    /**
     * Returns true if all of the fragments in the array are read-only
     * 
     * @param catalog_obj
     * @param fragments
     * @param cnt
     * @return
     */
    public static boolean areFragmentsReadOnly(CatalogType catalog_obj, long fragments[], int cnt) {
        for (int i = 0; i < cnt; i++) {
            if (isPlanFragmentReadOnly(fragments[i]))
                return (false);
        } // FOR
        return (true);
    }

}
