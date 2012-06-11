package edu.brown.catalog;

import junit.framework.TestCase;

public class TestPlanFragmentIdGenerator extends TestCase {
    
    private static final int BASE_ID = 1234;
    
    private void check(boolean readonly, boolean fastAggregate, boolean fastCombine) {
        int id = PlanFragmentIdGenerator.createPlanFragmentId(BASE_ID, readonly, fastAggregate, fastCombine);
        assert(id > 0);
        assertEquals(Integer.toString(id), readonly, PlanFragmentIdGenerator.isPlanFragmentReadOnly(id));
        assertEquals(Integer.toString(id), fastAggregate, PlanFragmentIdGenerator.isPlanFragmentFastAggregate(id));
        assertEquals(Integer.toString(id), fastCombine, PlanFragmentIdGenerator.isPlanFragmentFastCombine(id));
    }

    public void testIsPlanFragmentFastAggregate() {
        check(false, true, false);
    }
    
    public void testIsPlanFragmentFastCombine() {
        check(false, false, true);
    }
    
    public void testIsPlanFragmentReadOnly() {
        check(true, false, false);
    }
    
    public void testCheckAll() {
        check(true, true, true);
    }
}
