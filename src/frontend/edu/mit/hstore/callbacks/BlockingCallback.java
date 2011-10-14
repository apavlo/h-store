package edu.mit.hstore.callbacks;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.log4j.Logger;

import com.google.protobuf.RpcCallback;

import edu.brown.hstore.Hstore;
import edu.brown.utils.LoggerUtil;
import edu.brown.utils.Poolable;
import edu.brown.utils.LoggerUtil.LoggerBoolean;
import edu.mit.hstore.HStoreSite;

/**
 * 
 * @param <T> The message type of the original RpcCallback
 * @param <U> The message type that we will accumulate before invoking the original RpcCallback
 */
public abstract class BlockingCallback<T, U> implements RpcCallback<U>, Poolable {
    private static final Logger LOG = Logger.getLogger(BlockingCallback.class);
    private final static LoggerBoolean debug = new LoggerBoolean(LOG.isDebugEnabled());
    private final static LoggerBoolean trace = new LoggerBoolean(LOG.isTraceEnabled());
    static {
        LoggerUtil.attachObserver(LOG, debug, trace);
    }
    
    protected final HStoreSite hstore_site;
    private final AtomicInteger counter = new AtomicInteger(0);
    private RpcCallback<T> orig_callback;

    /**
     * We'll flip this flag if one of our partitions replies with an
     * unexpected abort. This ensures that we only send out the ABORT
     * to all the HStoreSites once. 
     */
    private final AtomicBoolean aborted = new AtomicBoolean(false);
    
    /**
     * Default Constructor
     */
    protected BlockingCallback(HStoreSite hstore_site) {
        this.hstore_site = hstore_site;
    }
    
    /**
     * The implementation of the run method to process a new entry for this callback
     * This method should return how much we should decrement from the blocking counter
     * @param parameter Needs to be >=0
     * @return
     */
    protected abstract int runImpl(U parameter);
    
    /**
     * This method is invoked once all of the T messages are recieved 
     */
    protected abstract void unblockCallback();
    
    /**
     * 
     */
    protected abstract void abortCallback(Hstore.Status status);
    
    /**
     * 
     */
    protected abstract void finishImpl();
    
    private void unblock() {
        if (debug.get())
            LOG.debug(String.format("Invoking %s.unblockCallback()", this.getClass().getSimpleName()));
        this.unblockCallback();
    }
    
    public final void abort(Hstore.Status status) {
        // If this is the first response that told us to abort, then we'll
        // send the abort message out 
        if (this.aborted.compareAndSet(false, true)) {
            this.abortCallback(status);
        }
    }
    
    @Override
    public void run(U parameter) {
        int counter = this.runImpl(parameter);
        
        // If this is the last result that we were waiting for, then we'll invoke
        // the unblockCallback()
        if (this.aborted.get() == false && this.counter.addAndGet(-1 * counter) == 0) {
            this.unblock();
        }
    }
    
    public boolean isAborted() {
        return (this.aborted.get());
    }
    
    protected final void init(int counter_val, RpcCallback<T> orig_callback) {
        assert(this.isInitialized() == false) : String.format("Trying to reuse %s before it is finished!", this.getClass().getSimpleName());
        if (debug.get())
            LOG.debug(String.format("Initialized new %s with counter = %d", this.getClass().getSimpleName(), counter_val));
        this.counter.set(counter_val);
        this.orig_callback = orig_callback;
    }
    
    @Override
    public boolean isInitialized() {
        return (this.orig_callback != null);
    }
    
    @Override
    public void finish() {
        this.aborted.set(false);
        this.orig_callback = null;
        this.finishImpl();
    }
    
    /**
     * This allows you to decrement the counter with actually
     * creating a message.
     */
    public void decrementCounter(int ctr) {
        if (debug.get())
            LOG.debug(String.format("Decrementing %s counter by %d", this.getClass().getSimpleName(), ctr));
        if (this.counter.addAndGet(-1 * ctr) == 0) {
            this.unblock();
        }
    }
    
    public int getCounter() {
        return this.counter.get();
    }
    public RpcCallback<T> getOrigCallback() {
        return this.orig_callback;
    }
}
