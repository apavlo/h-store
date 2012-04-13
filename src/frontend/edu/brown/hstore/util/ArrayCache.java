package edu.brown.hstore.util;

/**
 * Quick and easy integer array cache. The arrays will be 
 * reused indiscriminately, so therefore it should only be
 * used by one thread
 * @author pavlo
 */
public abstract class ArrayCache<T> {

    private T cache[];
    
    private ArrayCache(int size) {
        this.cache = this.createCache(size);
        for (int i = 0; i < size; i++) {
            this.cache[i] = this.createArray(i);
        } // FOR
    }
    
    protected abstract T[] createCache(int size);
    protected abstract T createArray(int size);
    
    /**
     * Retrieve an array that is given length
     * It will not be cleared out before being returned
     * The array does not need to be given back to the cache. 
     * @param length
     * @return
     */
    public final T getArray(int length) {
        if (length >= this.cache.length) {
            T temp[] = this.createCache(length+1);
            System.arraycopy(this.cache, 0, temp, 0, this.cache.length);
            this.cache = temp;
        }
        if (this.cache[length] == null) {
            this.cache[length] = this.createArray(length);
        }
        return (this.cache[length]);
    }
    
    /**
     * Fast Int Array Cache
     */
    public static class IntArrayCache extends ArrayCache<int[]> {
        public IntArrayCache(int size) {
            super(size);
        }
        @Override
        protected int[][] createCache(int size) {
            return (new int[size][]);
        }
        @Override
        protected int[] createArray(int size) {
            return (new int[size]);
        }
    } // CLASS
    
    /**
     * Fast Long Array Cache
     */
    public static class LongArrayCache extends ArrayCache<long[]> {
        public LongArrayCache(int size) {
            super(size);
        }
        @Override
        protected long[][] createCache(int size) {
            return (new long[size][]);
        }
        @Override
        protected long[] createArray(int size) {
            return (new long[size]);
        }
    } // CLASS
}
