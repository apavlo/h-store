package edu.brown.pools;

import java.util.NoSuchElementException;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.pool.BaseObjectPool;
import org.apache.commons.pool.PoolUtils;
import org.apache.commons.pool.PoolableObjectFactory;
import org.apache.log4j.Logger;

import edu.brown.logging.LoggerUtil;
import edu.brown.logging.LoggerUtil.LoggerBoolean;

/**
 * Based on org.apache.commons.pool.impl.StackObjectPool
 * @author pavlo
 * @param <T>
 */
public class FastObjectPool<T> extends BaseObjectPool {
    private static final Logger LOG = Logger.getLogger(FastObjectPool.class);
    private static final LoggerBoolean debug = new LoggerBoolean();
    private static final LoggerBoolean trace = new LoggerBoolean();
    static {
        LoggerUtil.attachObserver(LOG, debug, trace);
    }

    /**
     * The cap on the number of "sleeping" instances in the pool.
     */
    private static final int DEFAULT_MAX_SLEEPING  = 8;

    /**
     * The default initial size of the pool
     * (this specifies the size of the container, it does not
     * cause the pool to be pre-populated.)
     */
    private static final int DEFAULT_INIT_SLEEPING_CAPACITY = 4;
    
    /** 
     * My pool.
     */
    private Queue<T> pool = null;

    /** 
     * My {@link PoolableObjectFactory}.
     */
    private PoolableObjectFactory factory = null;
    
    /** 
     * The cap on the number of "sleeping" instances in the pool. 
     */
    private int maxSleeping = DEFAULT_MAX_SLEEPING;
    
    /**
     * Number of objects borrowed but not yet returned to the pool.
     */
    private final AtomicInteger numActive = new AtomicInteger(0);
    
    /**
     * Number of objects sitting in the pool
     */
    private final AtomicInteger numInactive = new AtomicInteger(0);
    
    // ----------------------------------------------------------------------------
    // CONSTRUCTORS
    // ----------------------------------------------------------------------------
    
    public FastObjectPool(PoolableObjectFactory factory) {
        this(factory, DEFAULT_MAX_SLEEPING, DEFAULT_INIT_SLEEPING_CAPACITY);
    }

    public FastObjectPool(PoolableObjectFactory factory, int idle) {
        this(factory, idle, DEFAULT_INIT_SLEEPING_CAPACITY);
    }

    public FastObjectPool(PoolableObjectFactory factory, int maxIdle, int initIdleCapacity) {
        this.factory = factory;
        maxSleeping = (maxIdle < 0 ? DEFAULT_MAX_SLEEPING : maxIdle);
        pool = new ConcurrentLinkedQueue<T>();
    }
    
    @SuppressWarnings("unchecked")
    @Override
    public T borrowObject() throws Exception {
        assertOpen();
        boolean newlyCreated = false;
        T obj = this.pool.poll();
        if (obj == null) {
            if (null == this.factory) {
                throw new NoSuchElementException();
            } else {
                obj = (T)this.factory.makeObject();
                if (obj == null) {
                    throw new NoSuchElementException("PoolableObjectFactory.makeObject() returned null.");
                }
                newlyCreated = true;
            }
        }
        else {
            this.numInactive.decrementAndGet();
        }
        assert(obj != null);
        try {
            this.factory.activateObject(obj);
            if (!this.factory.validateObject(obj)) {
                throw new Exception("ValidateObject failed");
            }
        } catch (Throwable ex) {
            PoolUtils.checkRethrow(ex);
            try {
                this.factory.destroyObject(obj);
            } catch (Throwable t2) {
                PoolUtils.checkRethrow(t2);
                // swallowed
            } finally {
                obj = null;
            }
            if (newlyCreated) {
                throw new NoSuchElementException(
                    "Could not create a validated object, cause: " +
                    ex.getMessage());
            }
        }
        this.numActive.incrementAndGet();
        
        if (debug.val)
            LOG.debug(String.format("Retrieved %s from ObjectPool [hashCode=%d]",
                      obj.getClass().getSimpleName(), obj.hashCode()));
            
        return obj;
    }
    
    @Override
    public void returnObject(Object obj) throws Exception {
        @SuppressWarnings("unchecked")
        T t = (T)obj;
        
        if (isClosed() || this.factory == null) return;
        boolean success = true;
        
        try {
            this.factory.passivateObject(obj);
        } catch(Exception e) {
            success = false;
        }

        boolean shouldDestroy = !success;
        this.numActive.decrementAndGet();
        int poolSize = this.numInactive.incrementAndGet();
        if (success) {
            Object toBeDestroyed = null;
            if (poolSize >= maxSleeping) {
                shouldDestroy = true;
                toBeDestroyed = this.pool.poll(); // remove the stalest object
            }
            if (debug.val)
                LOG.debug(String.format("Returning %s back to ObjectPool [hashCode=%d]",
                          t.getClass().getSimpleName(), t.hashCode()));
            this.pool.offer(t);
            // swap returned obj with the stalest one so it can be destroyed
            if (toBeDestroyed != null) obj = toBeDestroyed; 
        }

        if (shouldDestroy) { // by constructor, shouldDestroy is false when _factory is null
            try {
                this.factory.destroyObject(obj);
            } catch(Exception e) {
                // ignored
            }
        }
    }
    
    @Override
    public void invalidateObject(Object obj) throws Exception {
        this.numActive.decrementAndGet();
        if (null != factory) {
            this.factory.destroyObject(obj);
        }
    }

    /**
     * Return the number of instances
     * currently idle in this pool.
     *
     * @return the number of instances currently idle in this pool
     */
    public int getNumIdle() {
        return this.numInactive.get();
    }

    /**
     * Return the number of instances currently borrowed from this pool.
     *
     * @return the number of instances currently borrowed from this pool
     */
    public int getNumActive() {
        return this.numActive.get();
    }

    /**
     * Clears any objects sitting idle in the pool. Silently swallows any
     * exceptions thrown by {@link PoolableObjectFactory#destroyObject(Object)}.
     */
    public void clear() {
        if (null != factory) {
            T t = null;
            while ((t = this.pool.poll()) != null) {
                try {
                    this.factory.destroyObject(t);
                } catch(Exception e) {
                    // ignore error, keep destroying the rest
                }
            } // WHILE
            this.numInactive.lazySet(0);
        }
    }
    
    /**
     * Returns the {@link PoolableObjectFactory} used by this pool to create and manage object instances.
     * 
     * @return the factory
     * @since 1.5.5
     */
    public PoolableObjectFactory getFactory() {
        return this.factory;
    }

}
