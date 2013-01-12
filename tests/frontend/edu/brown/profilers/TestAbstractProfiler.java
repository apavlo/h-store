package edu.brown.profilers;

import org.junit.Test;

import junit.framework.TestCase;

/**
 * Simple tests for AbstractProfiler
 * @author pavlo
 */
public class TestAbstractProfiler extends TestCase {
    
    private static final int NUM_COUNTERS = 4;

    protected class MockProfiler extends AbstractProfiler {
        public final ProfileMeasurement pm0 = new ProfileMeasurement("PM0");
        public final ProfileMeasurement pm1 = new ProfileMeasurement("PM1");
        
        // Make some of these protected so that we can check getProfileMeasurements()
        protected final ProfileMeasurement pm2 = new ProfileMeasurement("PM2");
        protected final ProfileMeasurement pm3 = new ProfileMeasurement("PM3");
    }
    
    final MockProfiler profiler = new MockProfiler();
    
    /**
     * testGetTuple
     */
    @Test
    public void testGetTuple() throws Exception {
        ProfileMeasurement pms[] = profiler.getProfileMeasurements();
        ProfileMeasurement last = null;
        for (int i = 0; i < 10000; i++) {
            for (ProfileMeasurement pm : pms) {
                if (last != null) ProfileMeasurementUtil.swap(last, pm);
                else pm.start();
                last = pm;
            } // FOR
        } // FOR
        assertNotNull(last);
        last.stop();
        
        long tuple[] = profiler.getTuple();
        assertNotNull(tuple);
        assertEquals(NUM_COUNTERS*2, tuple.length);
        for (int i = 0; i < tuple.length; i++) {
            assertNotSame("OFFSET[" + i + "]", 0l, tuple[i]);
        } // FOR
    }
    
    /**
     * testGetProfileMeasurements
     */
    @Test
    public void testGetProfileMeasurements() throws Exception {
        ProfileMeasurement pms[] = profiler.getProfileMeasurements();
        assertNotNull(pms);
        assertEquals(NUM_COUNTERS, pms.length);
        for (int i = 0; i < pms.length; i++) {
            assertNotNull(Integer.toString(i), pms[i]);
        } // FOR
    }
    
    /**
     * testReset
     */
    @Test
    public void testReset() throws Exception {
        ProfileMeasurement pms[] = profiler.getProfileMeasurements();
        assertNotNull(pms);
        assertEquals(NUM_COUNTERS, pms.length);
        
        ProfileMeasurement last = null;
        for (int i = 0; i < 10000; i++) {
            for (ProfileMeasurement pm : pms) {
                if (last != null) ProfileMeasurementUtil.swap(last, pm);
                else pm.start();
                last = pm;
            } // FOR
        } // FOR
        assertNotNull(last);
        last.stop();
        for (ProfileMeasurement pm : pms) {
            assertTrue(pm.getName(), pm.getInvocations() > 0);
            assertFalse(pm.getName(), pm.isStarted());
        } // FOR
        
        profiler.reset();
        for (ProfileMeasurement pm : pms) {
            assertEquals(pm.getName(), 0, pm.getInvocations());
            assertEquals(pm.getName(), 0, pm.getTotalThinkTime());
            assertFalse(pm.getName(), pm.isStarted());
        } // FOR
    }
    
}
