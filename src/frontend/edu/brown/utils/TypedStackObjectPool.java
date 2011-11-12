package edu.brown.utils;

import org.apache.commons.pool.impl.StackObjectPool;
import org.apache.log4j.Logger;

import edu.brown.logging.LoggerUtil;
import edu.brown.logging.LoggerUtil.LoggerBoolean;

public class TypedStackObjectPool<T extends Poolable> extends StackObjectPool {
    private static final Logger LOG = Logger.getLogger(TypedStackObjectPool.class);
    private final static LoggerBoolean debug = new LoggerBoolean(LOG.isDebugEnabled());
    private final static LoggerBoolean trace = new LoggerBoolean(LOG.isTraceEnabled());
    static {
        LoggerUtil.attachObserver(LOG, debug, trace);
    }
    
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
        if (debug.get())
            LOG.debug(String.format("Returning %s back to ObjectPool [hashCode=%d]",
                                    t.getClass().getSimpleName(), t.hashCode()));
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
