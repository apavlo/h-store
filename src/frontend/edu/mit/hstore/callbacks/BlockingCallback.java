package edu.mit.hstore.callbacks;

import java.util.concurrent.atomic.AtomicInteger;

import com.google.protobuf.GeneratedMessage;
import com.google.protobuf.RpcCallback;

import edu.brown.hstore.Hstore;
import edu.brown.utils.Poolable;

public abstract class BlockingCallback<T, U extends GeneratedMessage> implements RpcCallback<U>, Poolable {
    
    private final AtomicInteger counter = new AtomicInteger(0);
    private RpcCallback<T> orig_callback;

    /**
     * Default Constructor
     */
    protected BlockingCallback() {
        // Nothing!
    }
    
    public abstract void unblockCallback();
    
    @Override
    public void run(U parameter) {
        // If this is the last PartitionResult that we were waiting for, then we'll send back
        // the TransactionWorkResponse to the remote HStoreSite
        if (this.getCounter().decrementAndGet() == 0) {
            this.unblockCallback();
        }
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
        this.orig_callback = null;
    }
    
    public AtomicInteger getCounter() {
        return this.counter;
    }
    public RpcCallback<T> getOrigCallback() {
        return this.orig_callback;
    }
}
