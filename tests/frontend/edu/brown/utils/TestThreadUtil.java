package edu.brown.utils;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import junit.framework.TestCase;

public class TestThreadUtil extends TestCase {

    public void testRun() {
        final int num_threads = 100;
        final AtomicInteger ctr = new AtomicInteger(0);
        
        List<Runnable> threads = new ArrayList<Runnable>(); 
        for (int i = 0; i < num_threads; i++) {
            threads.add(new Runnable() {
                public void run() {
                    ctr.incrementAndGet();
                };
            });
        } // FOR
        
        ThreadUtil.runNewPool(threads);
        assertEquals(num_threads, ctr.get());
    }
    
}
