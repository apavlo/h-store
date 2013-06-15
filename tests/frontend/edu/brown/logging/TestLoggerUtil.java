package edu.brown.logging;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import junit.framework.TestCase;

import org.junit.Test;

import edu.brown.benchmark.markov.RandomGenerator;

public class TestLoggerUtil extends TestCase {

//    private long rounds = 1000000000l;
    private long rounds =   10000000l;

    int size = 100;
    String array[] = new String[size];
    Map<Integer, String> map = new HashMap<Integer, String>();
    RandomGenerator r = new RandomGenerator();
    
    @Override
    protected void setUp() throws Exception {
        super.setUp();
        
        for (int i = 0; i < size; i++) {
            String val = r.astring(10, 64);
            array[i] = val;
            map.put(i, val);
        }
    }

    @Test
    public void testLoggerBoolean() {
        LoggerUtil.LoggerBoolean logger = new LoggerUtil.LoggerBoolean(true);
        for (long i = 0; i < rounds; i++) {
            assertTrue(logger.val);
        }
    }
    
    @Test
    public void testAtomicBoolean() {
        AtomicBoolean atomic = new AtomicBoolean(true);
        for (long i = 0; i < rounds; i++) {
            atomic.get();
        }
    }

    
//    public void testMapLookup() {
//        for (long i = 0; i < rounds; i++) {
//            Integer idx = Integer.valueOf(r.nextInt(size));
//            String temp = new String(map.get(idx).getBytes());
//            assertNotNull(temp);
//        }
//    }
//
//    public void testArrayLookup() {
//        for (long i = 0; i < rounds; i++) {
//            int idx = r.nextInt(size);
//            String temp = new String(map.get(idx).getBytes());
//            assertNotNull(temp);
//        }
//    }

}
