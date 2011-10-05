package edu.mit.hstore.callbacks;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import com.google.protobuf.RpcCallback;

import edu.brown.hstore.Hstore;
import edu.brown.utils.Poolable;

/**
 * 
 * @param <T> The message type of the original RpcCallback
 * @param <U> The message type that we will accumulate before invoking the original RpcCallback
 */
public abstract class BlockingCallback<T, U> implements RpcCallback<U>, Poolable {
    
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
    protected BlockingCallback() {
        // Nothing!
    }
    
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
    
    
    @Override
    public void run(U parameter) {
        // If this is the last PartitionResult that we were waiting for, then we'll send back
        // the TransactionWorkResponse to the remote HStoreSite
        if (this.getCounter().decrementAndGet() == 0) {
            this.unblockCallback();
        }
    }
    
    public final void abort(Hstore.Status status) {
        // If this is the first response that told us to abort, then we'll
        // send the abort message out 
        if (this.aborted.compareAndSet(false, true)) {
            this.abortCallback(status);
        }
    }
    
    public boolean isAborted() {
        return (this.aborted.get());
    }
    
    protected final void init(int counter_val, RpcCallback<T> orig_callback) {
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
    
    public AtomicInteger getCounter() {
        return this.counter;
    }
    public RpcCallback<T> getOrigCallback() {
        return this.orig_callback;
    }
}
