package edu.brown.utils;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.TreeSet;

import edu.brown.hstore.HStoreConstants;

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
    
    // --------------------------------------------------------------------------------------------
    // UTILITY METHODS
    // --------------------------------------------------------------------------------------------
    
    private void initialize(int num_elements) {
        for (int i = 0; i < num_elements; i++) {
            int p = rand.nextInt(NUM_PARTITIONS);
            pset.add(p);
            set.add(p);
        } // FOR
        assertEquals(set.isEmpty(), pset.isEmpty());
        assertEquals(set.size(), pset.size());
    }
    
    private void _removeAll(final Collection<Integer> to_remove) {
        int num_to_remove = rand.nextInt(set.size()-1);
        for (Integer p : set) {
            assertTrue(p.toString(), pset.contains(p));
            to_remove.add(p);
            if (to_remove.size() == num_to_remove) break;
        } // FOR

        boolean expected = set.removeAll(to_remove);
        boolean actual = pset.removeAll(to_remove);
        assert(expected);
        assertEquals(expected, actual);
        assertFalse(pset.isEmpty());
        assertEquals(set.size(), pset.size());
        
        for (Integer p : set) {
            assertTrue(p.toString(), pset.contains(p));
            assertFalse(p.toString(), to_remove.contains(p));
        } // FOR
        
        int values[] = pset.values();
        assertEquals(set.size(), values.length);
        for (int p : values) {
            assertTrue(Integer.toString(p), set.contains(p));
            assertFalse(Integer.toString(p), to_remove.contains(p));
        } // FOR
        
        // Trying to call removeAll again should return false
        expected = set.removeAll(to_remove);
        actual = pset.removeAll(to_remove);
        assertEquals(expected, actual);
        
        // Add in the NULL partition and make sure that doesn't get removed
        set.add(HStoreConstants.NULL_PARTITION_ID);
        pset.add(HStoreConstants.NULL_PARTITION_ID);
        expected = set.removeAll(to_remove);
        actual = pset.removeAll(to_remove);
        assertEquals(expected, actual);
        assertTrue(pset.contains(HStoreConstants.NULL_PARTITION_ID));
        
        // to_remove.clear();
        to_remove.add(HStoreConstants.NULL_PARTITION_ID);
        expected = set.removeAll(to_remove);
        actual = pset.removeAll(to_remove);
        assertEquals(expected, actual);
        assertFalse(pset.contains(HStoreConstants.NULL_PARTITION_ID));
    }
    
    // --------------------------------------------------------------------------------------------
    // TEST CASES
    // --------------------------------------------------------------------------------------------
    
    /**
     * testValues
     */
    public void testValues() {
        assertTrue(pset.isEmpty());
        this.initialize(rand.nextInt(NUM_PARTITIONS*3));
        int values[] = pset.values();
        assertEquals(set.size(), values.length);
        for (Integer partition : values) {
            assertTrue(partition.toString(), set.contains(partition));
        } // FOR
        
        // Add in the null guy
        pset.add(HStoreConstants.NULL_PARTITION_ID);
        set.add(HStoreConstants.NULL_PARTITION_ID);
        values = pset.values();
        assertEquals(set.size(), values.length);
        for (Integer partition : values) {
            assertTrue(partition.toString(), set.contains(partition));
        } // FOR
    }
    
    /**
     * testValues2
     */
    public void testValues2() {
        assertTrue(pset.isEmpty());
        for (int i = 0; i < 2; i++) {
            pset.add(i);
        } // FOR
        assertFalse(pset.isEmpty());
        assertEquals(2, pset.size());
        
        int values[] = pset.values();
        assertEquals(pset.size(), values.length);
        for (int partition : values) {
            assertFalse(Integer.toString(partition), set.contains(partition));
            
            set.add(partition);
            assertTrue(Integer.toString(partition), set.contains(partition));
            assertTrue(Integer.toString(partition), pset.contains(partition));
        } // FOR
        assertEquals(set.size(), pset.size());
    }
    
    /**
     * testToString
     */
    public void testToString() {
        for (int p = 0; p < NUM_PARTITIONS; p++) {
            assertFalse(pset.toString(), pset.contains(p));
            pset.add(p);
            String s = pset.toString();
            assertTrue(p+"->"+s, s.contains(Integer.toString(p)));
        } // FOR
        
        int p = HStoreConstants.NULL_PARTITION_ID;
        pset.add(p);
        String s = pset.toString();
        assertTrue(p+"->"+s, s.contains(Integer.toString(p)));
        // System.err.println(s);
    }
    
    /**
     * testIsEmptyNull
     */
    public void testIsEmptyNull() {
        assertTrue(pset.toString(), pset.isEmpty());
        assertTrue(set.toString(), set.isEmpty());
        
        pset.add(HStoreConstants.NULL_PARTITION_ID);
        set.add(HStoreConstants.NULL_PARTITION_ID);
        assertFalse(pset.toString(), pset.isEmpty());
        assertTrue(pset.toString(), pset.contains(HStoreConstants.NULL_PARTITION_ID));
        assertFalse(set.toString(), set.isEmpty());
        assertTrue(set.toString(), set.contains(HStoreConstants.NULL_PARTITION_ID));
        assertEquals(set.size(), pset.size());
        
        pset.clear();
        set.clear();
        assertTrue(pset.toString(), pset.isEmpty());
        assertTrue(set.toString(), set.isEmpty());
        assertEquals(set.size(), pset.size());
    }
    
    /**
     * testIsEmpty
     */
    public void testIsEmpty() {
        assertTrue(pset.toString(), pset.isEmpty());
        assertTrue(set.toString(), set.isEmpty());
        
        for (int p = 0; p < NUM_PARTITIONS; p++) {
            assertFalse(pset.toString(), pset.contains(p));
            assertFalse(set.toString(), set.contains(p));
            
            pset.add(p);
            set.add(p);
            assertFalse(pset.toString(), pset.isEmpty());
            assertTrue(pset.toString(), pset.contains(p));
            assertFalse(set.toString(), set.isEmpty());
            assertTrue(set.toString(), set.contains(p));
            assertEquals(set.size(), pset.size());
        } // FOR
        
        pset.clear();
        set.clear();
        assertTrue(pset.toString(), pset.isEmpty());
        assertTrue(set.toString(), set.isEmpty());
        assertEquals(set.size(), pset.size());
    }
    
    /**
     * testMinMax
     */
    public void testMinMax() {
        int min = 6;
        int max = 19;
        for (int i = min; i <= max; i++) {
            pset.add(i);
        }
        assertEquals(min, Collections.min(pset).intValue());
        assertEquals(max, Collections.max(pset).intValue());
    }
    
    /**
     * testNullPartitionId
     */
    public void testNullPartitionId() {
        assertFalse(pset.toString(), pset.contains(HStoreConstants.NULL_PARTITION_ID));
        pset.add(HStoreConstants.NULL_PARTITION_ID);
        assertTrue(pset.toString(), pset.contains(HStoreConstants.NULL_PARTITION_ID));
        for (Integer p : pset) {
            assertEquals(HStoreConstants.NULL_PARTITION_ID, p.intValue());
        }
        
        int num_elements = rand.nextInt(NUM_PARTITIONS);
        for (int i = 0; i < num_elements; i++) {
            int p = rand.nextInt(NUM_PARTITIONS);
            pset.add(p);
        } // FOR
        assertTrue(pset.toString(), pset.contains(HStoreConstants.NULL_PARTITION_ID));
        boolean found_null = false;
        for (Integer p : pset) {
            // System.err.println(p);
            if (p.intValue() == HStoreConstants.NULL_PARTITION_ID) {
                found_null = true;
            } else {
                assertNotSame(HStoreConstants.NULL_PARTITION_ID, p.intValue());
            }
        }
        assertTrue(pset.toString(), found_null);
        
        pset.remove(HStoreConstants.NULL_PARTITION_ID);
        assertFalse(pset.toString(), pset.contains(HStoreConstants.NULL_PARTITION_ID));
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
//            System.err.println(cnt + " - " + p);
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
        
        PartitionSet pset0 = new PartitionSet();
        PartitionSet pset1 = new PartitionSet();
        
        pset0.addAll(set);
        pset1.addAll(pset);
        assertEquals(pset0.size(), pset1.size());
        assertEquals(pset0, pset1);
    }
    
    /**
     * testRemoveAllCollection
     */
    public void testRemoveAllCollection() {
        this.initialize(rand.nextInt(NUM_PARTITIONS));
        this._removeAll(new TreeSet<Integer>());
    }
    
    /**
     * testRemoveAllPartitionSet
     */
    public void testRemoveAllPartitionSet() {
        this.initialize(rand.nextInt(NUM_PARTITIONS));
        this._removeAll(new PartitionSet());
    }
    
    /**
     * testParse
     */
    public void testParse() {
        PartitionSet expected = new PartitionSet(new int[]{ 0,1,2,3,4,5 });
        String parseStrs[] = {
            "0-5",
            "1,0-5",
            "0,1-5",
            "0, 1, 2, 3, 4, 5",
            "0,1, 2,3,4,5",
        };
        for (String s : parseStrs) {
            PartitionSet actual = PartitionSet.parse(s);
            assertEquals(expected, actual);
        } // FOR
    }
}
