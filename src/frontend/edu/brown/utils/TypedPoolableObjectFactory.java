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
public abstract class TypedPoolableObjectFactory<T extends Poolable> extends BasePoolableObjectFactory {

    private boolean enable_counting;
    private final AtomicInteger created = new AtomicInteger(0);
    private final AtomicInteger passivated = new AtomicInteger(0);
    private final AtomicInteger destroyed = new AtomicInteger(0);
    
    public TypedPoolableObjectFactory(boolean enable_counting) {
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
    
    /**
     * 
     * @param <X>
     * @param clazz
     * @param enable_tracking
     * @param args
     * @return
     */
    public static <X extends Poolable> TypedPoolableObjectFactory<X> makeFactory(final Class<X> clazz, final boolean enable_tracking, final Object...args) {
        Class<?> argsClazz[] = new Class[args.length];
        for (int i = 0; i < args.length; i++) {
            assert(args[i] != null) : "[" + i + "]";
            argsClazz[i] = args[i].getClass();
        } // FOR
        final Constructor<X> constructor = ClassUtil.getConstructor(clazz, argsClazz);
        return new TypedPoolableObjectFactory<X>(enable_tracking) {
            @Override
            public X makeObjectImpl() throws Exception {
                return (constructor.newInstance(args));
            }
        };
    }
}
