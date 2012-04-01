package edu.brown.hstore.util;

/**
 * Quick and easy integer array cache. The arrays will be 
 * reused indiscriminately, so therefore it should only be
 * used by one thread
 * @author pavlo
 */
public class IntArrayCache {

    private int cache[][];
    
    public IntArrayCache(int size) {
        this.cache = new int[size][];
        for (int i = 0; i < size; i++) {
            this.cache[i] = new int[i];
        } // FOR
    }
    
    /**
     * Retrieve an array that is given length
     * It will not be cleared out before being returned
     * The array does not need to be given back to the cache. 
     * @param length
     * @return
     */
    public int[] getArray(int length) {
        if (length >= cache.length) {
            int temp[][] = new int[length+1][];
            System.arraycopy(this.cache, 0, temp, 0, this.cache.length);
            this.cache = temp;
        }
        if (this.cache[length] == null) {
            this.cache[length] = new int[length];
        }
        return (this.cache[length]);
    }
    
}
