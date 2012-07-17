package edu.brown.profilers;

import java.util.Collection;

import org.junit.Test;

import junit.framework.TestCase;

/**
 * Simple tests for AbstractProfiler
 * @author pavlo
 */
public class TestAbstractProfiler extends TestCase {

    protected class MockProfiler extends AbstractProfiler {
        public final ProfileMeasurement pm0 = new ProfileMeasurement("PM0");
        public final ProfileMeasurement pm1 = new ProfileMeasurement("PM1");
        public final ProfileMeasurement pm2 = new ProfileMeasurement("PM2");
        public final ProfileMeasurement pm3 = new ProfileMeasurement("PM3");
    }
    
    final MockProfiler profiler = new MockProfiler();
    
    /**
     * testGetProfileMeasurements
     */
    @Test
    public void testGetProfileMeasurements() throws Exception {
        Collection<ProfileMeasurement> pms = profiler.getProfileMeasurements();
        assertNotNull(pms);
        assertEquals(4, pms.size());
    }
    
    /**
     * testReset
     */
    @Test
    public void testReset() throws Exception {
        ProfileMeasurement pms[] = profiler.getProfileMeasurements().toArray(new ProfileMeasurement[0]);
        assertNotNull(pms);
        assertEquals(4, pms.length);
        
        ProfileMeasurement last = null;
        for (int i = 0; i < 10000; i++) {
            for (ProfileMeasurement pm : pms) {
                if (last != null) ProfileMeasurement.swap(last, pm);
                else pm.start();
                last = pm;
            } // FOR
        } // FOR
        assertNotNull(last);
        last.stop();
        for (ProfileMeasurement pm : pms) {
            assertTrue(pm.getType(), pm.getInvocations() > 0);
            assertFalse(pm.getType(), pm.isStarted());
        } // FOR
        
        profiler.reset();
        for (ProfileMeasurement pm : pms) {
            assertEquals(pm.getType(), 0, pm.getInvocations());
            assertEquals(pm.getType(), 0, pm.getTotalThinkTime());
            assertFalse(pm.getType(), pm.isStarted());
        } // FOR
    }
    
}
