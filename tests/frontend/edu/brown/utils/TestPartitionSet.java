package edu.brown.utils;

import java.util.HashSet;
import java.util.Random;
import java.util.Set;

import junit.framework.TestCase;

public class TestPartitionSet extends TestCase {

    private static final int NUM_PARTITIONS = 100;
    private static final Random rand = new Random(0); 
    
    private final PartitionSet pset = new PartitionSet();
    private final Set<Integer> set = new HashSet<Integer>();
    
    @Override
    protected void setUp() throws Exception {
        super.setUp();
    }
    
    /**
     * testSize
     */
    public void testSize() {
        for (int p = 0; p < NUM_PARTITIONS; p++) {
            for (int i = 0; i < 5; i++) {
                pset.add(p);
                set.add(p);
                assertEquals(set.size(), pset.size());
            } // FOR
        } // FOR
    }
    
    /**
     * testIterator
     */
    public void testIterator() {
        assertTrue(pset.isEmpty());
        int num_elements = rand.nextInt(NUM_PARTITIONS*3);
        for (int i = 0; i < num_elements; i++) {
            int p = rand.nextInt(NUM_PARTITIONS);
            pset.add(p);
            set.add(p);
        } // FOR
        assertFalse(pset.isEmpty());
        assertEquals(set.size(), pset.size());
        
        int cnt = 0;
        for (Integer p : pset) {
            System.err.println(cnt + " - " + p);
            assertNotNull(pset.toString(), p);
            assertTrue(p.toString(), set.contains(p));
            cnt++;
        }
        assertEquals(set.size(), cnt);
    }
    
    /**
     * testAddAll
     */
    public void testAddAll() {
        int num_elements = rand.nextInt(NUM_PARTITIONS*3);
        for (int i = 0; i < num_elements; i++) {
            int p = rand.nextInt(NUM_PARTITIONS);
            set.add(p);
        } // FOR
        
        pset.addAll(set);
        assertFalse(pset.isEmpty());
        assertEquals(set.size(), pset.size());
    }
}
