package edu.brown.utils;

import org.apache.commons.pool.impl.StackObjectPool;

public class TypedStackObjectPool<T extends Poolable> extends StackObjectPool {
    
    public TypedStackObjectPool(TypedPoolableObjectFactory<T> factory) {
        super(factory);
    }
    
    public TypedStackObjectPool(TypedPoolableObjectFactory<T> factory, int idle) {
        super(factory, idle);
    }
    
    @SuppressWarnings("unchecked")
    @Override
    public synchronized T borrowObject() throws Exception {
        T t = (T)super.borrowObject();
        assert(t.isInitialized() == false) :
            String.format("Trying to reuse %s<%s> before it is finished!",
                          this.getClass().getSimpleName(), t);
        return t;
    }
    
    public void returnObject(T t) {
        try {
            super.returnObject(t);
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
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
        TypedPoolableObjectFactory<X> factory = TypedPoolableObjectFactory.makeFactory(clazz, enable_tracking, args);
        return new TypedStackObjectPool<X>(factory, idle);
    }
}
