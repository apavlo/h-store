package edu.brown.utils;

import org.apache.commons.pool.impl.StackObjectPool;

public class TypedStackObjectPool<T extends Poolable> extends StackObjectPool {
    
    public TypedStackObjectPool(CountingPoolableObjectFactory<T> factory) {
        super(factory);
    }
    
    public TypedStackObjectPool(CountingPoolableObjectFactory<T> factory, int idle) {
        super(factory, idle);
    }
    
    @SuppressWarnings("unchecked")
    @Override
    public synchronized T borrowObject() throws Exception {
        return (T)super.borrowObject();
    }

    /**
     * 
     * @param <X>
     * @param clazz
     * @param idle
     * @param enable_tracking
     * @param args
     * @return
     */
    public static <X extends Poolable> TypedStackObjectPool<X> factory(final Class<X> clazz, final int idle, final boolean enable_tracking, final Object...args) {
        CountingPoolableObjectFactory<X> factory = CountingPoolableObjectFactory.makeFactory(clazz, enable_tracking, args);
        return new TypedStackObjectPool<X>(factory, idle);
    }
}
