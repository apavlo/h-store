package edu.brown.hstore.util;

import java.util.Arrays;

import edu.brown.hstore.util.ArrayCache.IntArrayCache;
import edu.brown.hstore.util.ArrayCache.LongArrayCache;
import junit.framework.TestCase;

public class TestArrayCache extends TestCase {

    private static final int INITIAL_SIZE = 11;
    private static final int MAX_SIZE = 33;
    
    /**
     * testIntArrayCache
     */
    public void testIntArrayCache() throws Exception {
        IntArrayCache cache = new IntArrayCache(INITIAL_SIZE);
        for (int i = 1; i < MAX_SIZE; i++) {
            int arr[] = cache.getArray(i);
            assertNotNull(arr);
            assertEquals(i, arr.length);
            Arrays.fill(arr, i);
        } // FOR
        
        // Make sure that we are reusing the arrays
        for (int i = 1; i < MAX_SIZE; i++) {
            int arr[] = cache.getArray(i);
            assertNotNull(arr);
            assertEquals(i, arr.length);
            for (int ii = 0; ii < arr.length; ii++)
                assertEquals(i, arr[ii]);
        } // FOR
    }
    
    /**
     * testLongArrayCache
     */
    public void testLongArrayCache() throws Exception {
        LongArrayCache cache = new LongArrayCache(INITIAL_SIZE);
        for (int i = 1; i < MAX_SIZE; i++) {
            long arr[] = cache.getArray(i);
            assertNotNull(arr);
            assertEquals(i, arr.length);
            Arrays.fill(arr, i);
        } // FOR
        
        // Make sure that we are reusing the arrays
        for (int i = 1; i < MAX_SIZE; i++) {
            long arr[] = cache.getArray(i);
            assertNotNull(arr);
            assertEquals(i, arr.length);
            for (int ii = 0; ii < arr.length; ii++)
                assertEquals(i, arr[ii]);
        } // FOR
    }
    
}
