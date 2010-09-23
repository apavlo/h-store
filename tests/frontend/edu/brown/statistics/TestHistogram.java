package edu.brown.statistics;

import org.junit.Test;
import java.lang.reflect.Field;
import java.util.*;

import org.json.*;

import edu.brown.BaseTestCase;

public class TestHistogram extends BaseTestCase {

    public static final int NUM_PARTITIONS = 100;
	public static final int NUM_SAMPLES = 100;
	public static final int RANGE = 20;
	public static final double SKEW_FACTOR = 4.0;
	
	private Histogram h = new Histogram();
	private Random rand = new Random(1);
	
	protected void setUp() throws Exception {
	    
	    
	    // Cluster a bunch in the center
	    int min = RANGE / 3;
	    for (int i = 0; i < NUM_SAMPLES; i++) {
            h.put((long)(rand.nextInt(min) + min));
        }
		for (int i = 0; i < NUM_SAMPLES; i++) {
			h.put((long)(rand.nextInt(RANGE)));
		}
	}
	
	/**
	 * testZeroEntries
	 */
	@Test
	public void testZeroEntries() {
	    Set<Long> attempted = new HashSet<Long>();
	    
	    // First try to add a bunch of zero entries and make sure that they aren't included
	    // in the list of values stored in the histogram
	    h.setKeepZeroEntries(false);
	    assertFalse(h.isZeroEntriesEnabled());
	    for (int i = 0; i < NUM_SAMPLES; i++) {
	        long key = 0;
	        do {
	            key = rand.nextInt();
	        } while (h.contains(key) || attempted.contains(key));
	        h.put(key, 0);
	        attempted.add(key);
	    } // FOR
	    for (Long key : attempted) {
	        assertFalse(h.contains(key));
	        assertNull(h.get(key));
	    } // FOR
	    
	    // Now enable zero entries and make sure that our entries make it in there
	    h.setKeepZeroEntries(true);
	    assert(h.isZeroEntriesEnabled());
	    for (Long key : attempted) {
	        h.put(key, 0);
	        assert(h.contains(key));
	        assertEquals(0, h.get(key).longValue());
	    } // FOR
	    
	    // Disable zero entries again and make sure that our entries from the last step are removed
	    h.setKeepZeroEntries(false);
	    assertFalse(h.isZeroEntriesEnabled());
	    for (Long key : attempted) {
	        assertFalse(h.contains(key));
	        assertNull(h.get(key));
	    } // FOR
	}
	
	/**
	 * testPutValues
	 */
	@Test
	public void testPutValues() {
	    Histogram hist = new Histogram();
	    hist.put(49);
	    hist.put(50);
	    
	    List<Integer> partitions = new ArrayList<Integer>();
	    for (int i = 0; i < NUM_PARTITIONS; i++)
	        partitions.add(i);
	    
	    hist.putValues(partitions);
	    assertEquals(partitions.size(), hist.getValueCount());
	    assertTrue(hist.values().containsAll(partitions));
	    
	    for (int i : partitions) {
	        assertNotNull(hist.get(i));
	        int cnt = hist.get(i).intValue();
	        int expected = (i == 49 || i == 50 ? 2 : 1);
            assertEquals(expected, cnt);
	    } // FOR
	}
	
	/**
	 * testSkewed
	 */
	@Test
	public void testSkewed() {
	    assertTrue(h.isSkewed(SKEW_FACTOR));
	}

    /**
     * testToJSONString
     */
    public void testToJSONString() throws Exception {
        String json = h.toJSONString();
        assertNotNull(json);
        for (Histogram.Members element : Histogram.Members.values()) {
            assertTrue(json.indexOf(element.name()) != -1);
        } // FOR
    }
    
    /**
     * testFromJSON
     *
     **/
    public void testFromJSON() throws Exception {
        String json = h.toJSONString();
        assertNotNull(json);
        JSONObject jsonObject = new JSONObject(json);
        
        Histogram copy = new Histogram();
        copy.fromJSON(jsonObject, null);
        assertEquals(h.histogram.size(), copy.histogram.size());
        for (Histogram.Members element : Histogram.Members.values()) {
            String field_name = element.toString().toLowerCase();
            Field field = Histogram.class.getDeclaredField(field_name);
            assertNotNull(field);
            
            Object orig_value = field.get(h);
            Object copy_value = field.get(copy);
            
            if (element == Histogram.Members.HISTOGRAM) {
                for (Object value : h.histogram.keySet()) {
                    assertNotNull(value);
                    assertEquals(h.get(value), copy.get(value));
                } // FOR
            } else {
                assertEquals(orig_value.toString(), copy_value.toString());
            }
        } // FOR
    }
}
