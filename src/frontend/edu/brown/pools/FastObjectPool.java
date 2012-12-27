package edu.brown.pools;

import java.util.LinkedList;
import java.util.NoSuchElementException;
import java.util.Queue;
// import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.pool.BaseObjectPool;
import org.apache.commons.pool.PoolUtils;
import org.apache.commons.pool.PoolableObjectFactory;
import org.apache.log4j.Logger;

import edu.brown.logging.LoggerUtil;
import edu.brown.logging.LoggerUtil.LoggerBoolean;

/**
 *
 * Based on org.apache.commons.pool.impl.StackObjectPool
 * @author pavlo
 * @param <T>
 */
public class FastObjectPool<T> extends BaseObjectPool {
    private static final Logger LOG = Logger.getLogger(FastObjectPool.class);
    private static final LoggerBoolean debug = new LoggerBoolean(LOG.isDebugEnabled());
    private static final LoggerBoolean trace = new LoggerBoolean(LOG.isTraceEnabled());
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
    private Queue<T> _pool = null;

    /** 
     * My {@link PoolableObjectFactory}.
     */
    private PoolableObjectFactory _factory = null;
    
    /** 
     * The cap on the number of "sleeping" instances in the pool. 
     */
    private int _maxSleeping = DEFAULT_MAX_SLEEPING;
    
    /**
     * Number of objects borrowed but not yet returned to the pool.
     */
    private final AtomicInteger _numActive = new AtomicInteger(0);
    
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
        this._factory = factory;
        _maxSleeping = (maxIdle < 0 ? DEFAULT_MAX_SLEEPING : maxIdle);
        _pool = new LinkedList<T>();
    }
    
    @SuppressWarnings("unchecked")
    @Override
    public T borrowObject() throws Exception {
        assertOpen();
        boolean newlyCreated = false;
        T obj = _pool.poll();
        if (obj == null) {
            if (null == _factory) {
                throw new NoSuchElementException();
            } else {
                obj = (T)_factory.makeObject();
                if (obj == null) {
                    throw new NoSuchElementException("PoolableObjectFactory.makeObject() returned null.");
                }
                newlyCreated = true;
            }
        }
        assert(obj != null);
        try {
            _factory.activateObject(obj);
            if (!_factory.validateObject(obj)) {
                throw new Exception("ValidateObject failed");
            }
        } catch (Throwable ex) {
            PoolUtils.checkRethrow(ex);
            try {
                _factory.destroyObject(obj);
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
        _numActive.incrementAndGet();
        
        if (debug.get())
            LOG.debug(String.format("Retrieved %s from ObjectPool [hashCode=%d]",
                      obj.getClass().getSimpleName(), obj.hashCode()));
            
        return obj;
    }
    
    @Override
    public void returnObject(Object obj) throws Exception {
        @SuppressWarnings("unchecked")
        T t = (T)obj;
        
        if (isClosed() || _factory == null) return;
        boolean success = true;
        
        try {
            _factory.passivateObject(obj);
        } catch(Exception e) {
            success = false;
        }
        
//        if (!_factory.validateObject(obj)) {
//            success = false;
//        } else {
//            
//        }

        boolean shouldDestroy = !success;
        this._numActive.decrementAndGet();
        if (success) {
            Object toBeDestroyed = null;
            if (_pool.size() >= _maxSleeping) {
                shouldDestroy = true;
                toBeDestroyed = _pool.poll(); // remove the stalest object
            }
            if (debug.get())
                LOG.debug(String.format("Returning %s back to ObjectPool [hashCode=%d]",
                                        t.getClass().getSimpleName(), t.hashCode()));
            _pool.offer(t);
            // swap returned obj with the stalest one so it can be destroyed
            if (toBeDestroyed != null) obj = toBeDestroyed; 
        }

        if (shouldDestroy) { // by constructor, shouldDestroy is false when _factory is null
            try {
                _factory.destroyObject(obj);
            } catch(Exception e) {
                // ignored
            }
        }
    }
    
    @Override
    public void invalidateObject(Object obj) throws Exception {
        _numActive.decrementAndGet();
        if (null != _factory) {
            _factory.destroyObject(obj);
        }
    }

    /**
     * Return the number of instances
     * currently idle in this pool.
     *
     * @return the number of instances currently idle in this pool
     */
    public int getNumIdle() {
        return _pool.size();
    }

    /**
     * Return the number of instances currently borrowed from this pool.
     *
     * @return the number of instances currently borrowed from this pool
     */
    public int getNumActive() {
        return _numActive.get();
    }

    /**
     * Clears any objects sitting idle in the pool. Silently swallows any
     * exceptions thrown by {@link PoolableObjectFactory#destroyObject(Object)}.
     */
    public void clear() {
        if (null != _factory) {
            T t = null;
            while ((t = _pool.poll()) != null) {
                try {
                    _factory.destroyObject(t);
                } catch(Exception e) {
                    // ignore error, keep destroying the rest
                }
            } // WHILE
        }
    }
    
    /**
     * Returns the {@link PoolableObjectFactory} used by this pool to create and manage object instances.
     * 
     * @return the factory
     * @since 1.5.5
     */
    public PoolableObjectFactory getFactory() {
        return _factory;
    }

}
