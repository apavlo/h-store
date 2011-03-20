package edu.brown.hashing;

import java.util.HashMap;
import java.util.Map;

import org.junit.Test;
import org.voltdb.catalog.Database;

import edu.brown.BaseTestCase;
import edu.brown.rand.AbstractRandomGenerator;
import edu.brown.rand.DefaultRandomGenerator;

public class TestCachedHasher extends BaseTestCase {

    private static final int NUM_PARTITIONS = 200;
    private static final int ITERATIONS = 100000000;
//    private static final int SIZE = 10;
    private final AbstractRandomGenerator rand = new DefaultRandomGenerator(100);
    
    @Override
    protected void setUp() throws Exception {
        super.setUp();
        
    }

    private void burnCycles(AbstractHasher hasher) {
        int max = 1000;
        int iterations = ITERATIONS;
        
        while (iterations-- > 0) {
            for (long i = 0; i < max; i++) {
                hasher.hash(i, NUM_PARTITIONS);
            } // FOR
        } // WHILE
    }
    
    @Test
    public void testDefaultHasher() {
        AbstractHasher hasher = new DefaultHasher(null, NUM_PARTITIONS);
        this.burnCycles(hasher);
    }
    
    @Test
    public void testCachedHasher() {
        AbstractHasher hasher = new CachedHasher(null, NUM_PARTITIONS);
        this.burnCycles(hasher);
    }
    
//    public void testHashMap() {
//        Map<Integer, String> m = new HashMap<Integer, String>();
//        for (int i = 0; i < SIZE; i++) {
//            m.put(i, rand.astring(64, 256));
//        } // FOR
//        
//        int iterations = ITERATIONS;
//        while (iterations-- > 0) {
//            int idx = rand.nextInt(SIZE);
//            assertNotNull(m.get(idx));
//        } // WHILE
//    }
//    
//    public void testArray() {
//        String a[] = new String[SIZE * 3];
//        for (int i = 0; i < SIZE; i++) {
//            a[i] = rand.astring(64, 256);
//        } // FOR
//        int iterations = ITERATIONS;
//        while (iterations-- > 0) {
//            int idx = rand.nextInt(SIZE);
//            assertNotNull(a[idx]);
//        } // WHILE
//    }
    
}
