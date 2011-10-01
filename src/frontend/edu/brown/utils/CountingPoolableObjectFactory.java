package edu.brown.utils;

import java.lang.reflect.Constructor;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.pool.BasePoolableObjectFactory;

/**
 * 
 * @author pavlo
 *
 * @param <T>
 */
public abstract class CountingPoolableObjectFactory<T extends Poolable> extends BasePoolableObjectFactory {

    private boolean enable_counting;
    private final AtomicInteger created = new AtomicInteger(0);
    private final AtomicInteger passivated = new AtomicInteger(0);
    private final AtomicInteger destroyed = new AtomicInteger(0);
    
    public CountingPoolableObjectFactory(boolean enable_counting) {
        this.enable_counting = enable_counting;
    }
    
    public abstract T makeObjectImpl() throws Exception;
    
    @Override
    public final Object makeObject() throws Exception {
        Object obj = this.makeObjectImpl();
        if (this.enable_counting) this.created.getAndIncrement();
        return obj;
    }
    
    @Override
    public final void passivateObject(Object obj) throws Exception {
        ((Poolable)obj).finish();
        if (this.enable_counting) this.passivated.getAndIncrement();
    }
    
    @Override
    public final void destroyObject(Object obj) throws Exception {
        if (this.enable_counting) this.destroyed.getAndIncrement();
    }
    
    public int getCreatedCount() {
        return (this.created.get());
    }
    public int getPassivatedCount() {
        return (this.passivated.get());
    }
    public int getDestroyedCount() {
        return (this.destroyed.get());
    }
    
    public static <X extends Poolable> CountingPoolableObjectFactory<X> makeFactory(final Class<X> clazz, final boolean enable_tracking) {
        return new CountingPoolableObjectFactory<X>(enable_tracking) {
            private final Constructor<X> constructor;
            {
                this.constructor = ClassUtil.getConstructor(clazz);
            }
            @Override
            public X makeObjectImpl() throws Exception {
                return (this.constructor.newInstance());
            }
        };
    }
}
