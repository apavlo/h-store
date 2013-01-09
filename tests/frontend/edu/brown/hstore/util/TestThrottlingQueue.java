package edu.brown.hstore.util;

import java.util.LinkedList;
import java.util.Queue;

import edu.brown.BaseTestCase;
import edu.brown.rand.DefaultRandomGenerator;

/**
 * 
 * @author pavlo
 */
public class TestThrottlingQueue extends BaseTestCase {

    private static final int QUEUE_THRESHOLD = 20;
    private static final double QUEUE_RELEASE = 0.75;
    private static final int QUEUE_AUTO_DELTA = 2;
    private static final int QUEUE_AUTO_MIN = QUEUE_THRESHOLD - QUEUE_AUTO_DELTA;
    private static final int QUEUE_AUTO_MAX = QUEUE_THRESHOLD * 2;
    
    ThrottlingQueue<String> queue;
    final DefaultRandomGenerator rng = new DefaultRandomGenerator();
    
    @Override
    protected void setUp() throws Exception {
        super.setUp();
        
        Queue<String> inner = new LinkedList<String>();
        this.queue = new ThrottlingQueue<String>(inner,
                                                 QUEUE_THRESHOLD,
                                                 QUEUE_RELEASE,
                                                 QUEUE_AUTO_DELTA,
                                                 QUEUE_AUTO_MIN,
                                                 QUEUE_AUTO_MAX);
        this.queue.setAllowDecrease(false);
        this.queue.setAllowIncrease(false);
    }
    
    // --------------------------------------------------------------------------------------------
    // UTILITY METHODS
    // --------------------------------------------------------------------------------------------
    
    private void fillAndDrain(int count) throws Exception {
        boolean should_be_throttled = count >= queue.getThrottleThreshold();
        assertTrue(queue.isEmpty());
        for (int i = 0; i < count; i++) {
            assertTrue(queue.debug(), queue.offer(rng.astring(10, 20)));
        } // FOR
        assertEquals(queue.debug(), count, queue.size());
        assertEquals(queue.debug(), should_be_throttled, queue.isThrottled());
        
        while (queue.isEmpty() == false) {
            assertNotNull(queue.poll());
        } // WHILE
        assertFalse(queue.debug(), queue.isThrottled());
    }
    
    // --------------------------------------------------------------------------------------------
    // TEST CASES
    // --------------------------------------------------------------------------------------------
    
    /**
     * testCheckThrottling
     */
    public void testCheckThrottling() throws Exception {
        boolean ret;
        
        // Just add a bunch of items and check to see that the last
        // one gets rejected after we add more than QUEUE_MAX items
        while (queue.isThrottled() == false) {
            ret = queue.offer(rng.astring(10, 20));
            assertTrue("Size: " + queue.size(), ret);
        } // FOR
        assertEquals(QUEUE_THRESHOLD, queue.size());
        assertEquals(queue.size(), queue.getQueue().size());
        
        // Now if we can't add a new one until we pop off 
        // enough items that put us below the release level
        while (queue.size() > queue.getThrottleRelease()) {
            ret = queue.offer(rng.astring(10, 20));
            assertFalse(queue.debug(), ret);
            assertTrue(queue.debug(), queue.isThrottled());
            
            String item = queue.poll();
            assertNotNull(item);
        } // WHILE

        ret = queue.offer(rng.astring(10, 20));
        assertTrue(queue.debug(), ret);
    }
    
    /**
     * testAutoDecrease
     */
    public void testAutoDecrease() throws Exception {
        // Enable automatic decreasing of the throttling threshold.
        // Then add in a bunch of txns to get us throttled
        // Check that both the throttling threshold and release threshold
        // get decreased.
        queue.setAllowDecrease(true);
        queue.setAllowIncrease(false);
        int origThreshold = queue.getThrottleThreshold();
        assertEquals(QUEUE_THRESHOLD, origThreshold);
        int origRelease = queue.getThrottleRelease();
        
        this.fillAndDrain(QUEUE_THRESHOLD);
        this.fillAndDrain(queue.getThrottleThreshold());
        System.err.println(queue.debug());

        int newThreshold = queue.getThrottleThreshold();
        assertTrue(newThreshold+"<"+origThreshold, newThreshold < origThreshold);
        assertEquals(queue.debug(), QUEUE_AUTO_MIN, newThreshold);
        int newRelease = queue.getThrottleRelease();
        assertTrue(queue.debug(), newRelease < origRelease);
        assertTrue(queue.debug(), newRelease < newThreshold);
    }
    
    /**
     * testAutoIncrease
     */
    public void testAutoIncrease() throws Exception {
        // Enable automatic increasing of the throttling threshold.
        // Then add in a bunch of txns, then clear it out by polling.
        // Check that both the throttling threshold and release threshold
        // get increased after the queue is empty.
        queue.setAllowIncrease(true);
        int origThreshold = queue.getThrottleThreshold();
        assertEquals(QUEUE_THRESHOLD, origThreshold);
        int origRelease = queue.getThrottleRelease();
        
        this.fillAndDrain(QUEUE_THRESHOLD);

        int newThreshold = queue.getThrottleThreshold();
        assertTrue(origThreshold < newThreshold);
        assertEquals(queue.debug(), QUEUE_THRESHOLD + QUEUE_AUTO_DELTA, newThreshold);
        int newRelease = queue.getThrottleRelease();
        assertTrue(queue.debug(), origRelease < newRelease);
        assertTrue(queue.debug(), newRelease < newThreshold);
    }
    
    /**
     * testAutoIncreaseMax
     */
    public void testAutoIncreaseMax() throws Exception {
        // Enable automatic increasing of the throttling threshold.
        // Do a bunch of fills and drains, but make sure that we never go
        // above the max threshold
        queue.setAllowIncrease(true);
        
        for (int i = 0; i < 100; i++) {
            this.fillAndDrain(QUEUE_THRESHOLD);
        }
        
        int newThreshold = queue.getThrottleThreshold();
        assertEquals(queue.debug(), QUEUE_AUTO_MAX, newThreshold);
        int newRelease = queue.getThrottleRelease();
        assertTrue(queue.debug(), newRelease < newThreshold);
    }
    
}
