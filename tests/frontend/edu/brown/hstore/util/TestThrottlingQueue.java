package edu.brown.hstore.util;

import java.util.concurrent.PriorityBlockingQueue;

import edu.brown.BaseTestCase;
import edu.brown.rand.DefaultRandomGenerator;

public class TestThrottlingQueue extends BaseTestCase {

    static final int QUEUE_MAX = 20;
    static final double QUEUE_RELEASE = 0.75;
    static final int QUEUE_INCREASE = 1;
    static final int QUEUE_INCREASE_MAX = QUEUE_MAX * 5;
    
    ThrottlingQueue<String> queue;
    final DefaultRandomGenerator rng = new DefaultRandomGenerator();
    
    @Override
    protected void setUp() throws Exception {
        super.setUp();
        
        
        PriorityBlockingQueue<String> inner = new PriorityBlockingQueue<String>();
        this.queue = new ThrottlingQueue<String>(inner,
                                                  QUEUE_MAX,
                                                  QUEUE_RELEASE,
                                                  QUEUE_INCREASE,
                                                  QUEUE_INCREASE_MAX);
    }
    
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
        assertEquals(QUEUE_MAX, queue.size());
        assertEquals(queue.size(), queue.getQueue().size());
        
        // Now if we can't add a new one until we pop off 
        // enough items that put us below the release level
        while (queue.size() > queue.getThrottleRelease()) {
            ret = queue.offer(rng.astring(10, 20));
            assertFalse("Size: " + queue.size() + " / " +
                        "Release: " + queue.getThrottleRelease(), ret);
            assertTrue(queue.isThrottled());
            
            String item = this.queue.poll();
            assertNotNull(item);
        } // WHILE

        ret = this.queue.offer(rng.astring(10, 20));
        assertTrue("Size: " + this.queue.size(), ret);
    }
    
//    /**
//     * testThrottleRelease
//     */
//    public void testThrottleRelease() throws Exception {
//        boolean ret;
//        
//        // Just add a bunch of items and check to see that the last
//        // one gets rejected after we add more than QUEUE_MAX items
//        for (int i = 0; i < QUEUE_MAX; i++) {
//            ret = this.queue.offer(rng.astring(10, 20));
//            assertTrue("Size: " + this.queue.size(), ret);
//        } // FOR
//        assertEquals(QUEUE_MAX, this.queue.size());
//        
//        ret = this.queue.offer(rng.astring(10, 20));
//        assertFalse("Size: " + this.queue.size(), ret);
//        assertEquals(QUEUE_MAX, this.queue.size());
//        
//        // Now if we pop one off, then we can add a new one
//        String item = this.queue.poll();
//        assertNotNull(item);
//        
//        ret = this.queue.offer(rng.astring(10, 20));
//        assertTrue("Size: " + this.queue.size(), ret);
//        assertEquals(QUEUE_MAX, this.queue.size());
//    }
    
    
    
}
