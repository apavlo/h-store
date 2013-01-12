package edu.brown.profilers;

import java.util.HashMap;
import java.util.Map;

import org.junit.Test;

import junit.framework.TestCase;

public class TestTransactionProfiler extends TestCase {

    final TransactionProfiler profiler = new TransactionProfiler();
    
    /**
     * testGetProfileMeasurements
     */
    @Test
    public void testGetProfileMeasurements() throws Exception {
        // Make sure all of our ProfileMeasurements have unique type names
        Map<String, Integer> fields = new HashMap<String, Integer>();
        ProfileMeasurement pms[] = profiler.getProfileMeasurements();
        for (ProfileMeasurement pm : pms) {
            assertNotNull(pm);
            assertFalse(pm.getName(), fields.containsKey(pm.getName()));
            fields.put(pm.getName(), pm.hashCode());
        } // FOR
        
        // Then just make sure that we don't get the same handles for a new object
        TransactionProfiler other = new TransactionProfiler();
        for (ProfileMeasurement pm : other.getProfileMeasurements()) {
            assertNotNull(pm);
            assertTrue(pm.getName(), fields.containsKey(pm.getName()));
            assertFalse(pm.getName(), fields.get(pm.getName()) == pm.hashCode());
        } // FOR
    }
    
}
