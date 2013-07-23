package edu.brown.hstore.callbacks;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.log4j.Logger;

import com.google.protobuf.RpcCallback;

import edu.brown.hstore.Hstoreservice.Status;
import edu.brown.logging.LoggerUtil;
import edu.brown.logging.LoggerUtil.LoggerBoolean;
import edu.brown.pools.Poolable;
import edu.brown.hstore.HStoreSite;
import edu.brown.hstore.conf.HStoreConf;

/**
 * 
 * @param <T> The message type of the original RpcCallback
 * @param <U> The message type that we will accumulate before invoking the original RpcCallback
 */
public abstract class BlockingRpcCallback<T, U> implements RpcCallback<U>, Poolable {
    private static final Logger LOG = Logger.getLogger(BlockingRpcCallback.class);
    private static final LoggerBoolean debug = new LoggerBoolean();
    private static final LoggerBoolean trace = new LoggerBoolean();
    static {
        LoggerUtil.attachObserver(LOG, debug, trace);
    }
    
    protected final HStoreSite hstore_site;
    protected final HStoreConf hstore_conf;
    protected Long txn_id = null;
    private final AtomicInteger counter = new AtomicInteger(0);
    
    // We retain the original parameters of the last init() for debugging
    private Long orig_txn_id = null;
    private int orig_counter;
    private RpcCallback<T> orig_callback;
    
    /**
     * We'll flip this flag if one of our partitions replies with an
     * unexpected abort. This ensures that we only send out the ABORT
     * to all the HStoreSites once. 
     */
    private final AtomicBoolean abortInvoked = new AtomicBoolean(false);
    
    /**
     * This flag is set to true when the unblockCallback() is invoked
     */
    private final AtomicBoolean unblockInvoked = new AtomicBoolean(false);
    
    /**
     * This flag is set to true if the callback has been cancelled
     */
    private final AtomicBoolean canceled = new AtomicBoolean(false);
    
    /**
     * If set to true, then this callback will still invoke unblockCallback()
     * once all of the messages arrive
     */
    private final boolean invoke_even_if_aborted;
    
    /**
     * Constructor
     * If invoke_even_if_aborted set to true, then this callback will still execute
     * the unblockCallback() method after all the responses have arrived. 
     * @param invoke_even_if_aborted  
     */
    protected BlockingRpcCallback(HStoreSite hstore_site, boolean invoke_even_if_aborted) {
        this.hstore_site = hstore_site;
        this.hstore_conf = hstore_site.getHStoreConf();
        this.invoke_even_if_aborted = invoke_even_if_aborted;
    }
    
    /**
     * Initialize the BlockingCallback's counter and transaction info
     * @param txn_id
     * @param counter_val
     * @param orig_callback
     */
    protected void init(Long txn_id, int counter_val, RpcCallback<T> orig_callback) {
        if (debug.val) 
            LOG.debug(String.format("Txn #%d - Initialized new %s with counter = %d [hashCode=%d]",
                                    txn_id, this.getClass().getSimpleName(), counter_val, this.hashCode()));
        this.orig_counter = counter_val;
        this.counter.set(counter_val);
        this.orig_callback = orig_callback;
        this.txn_id = txn_id;
        this.orig_txn_id = txn_id;
    }
    
    @Override
    public boolean isInitialized() {
        return (this.orig_callback != null);
    }

    protected final Long getTransactionId() {
        return (this.txn_id);
    }
    /**
     * Return the current state of this callback's internal counter
     */
    public final int getCounter() {
        return (this.counter.get());
    }
    
    protected final Long getOrigTransactionId() {
        return (this.orig_txn_id);
    }
    protected final int getOrigCounter() {
        return (this.orig_counter);
    }
    protected final void clearCounter() {
        this.counter.set(0);
    }
    protected final RpcCallback<T> getOrigCallback() {
        return this.orig_callback;
    }

    // ----------------------------------------------------------------------------
    // RUN
    // ----------------------------------------------------------------------------
    
    @Override
    public final void run(U parameter) {
        int delta = this.runImpl(parameter);
        int new_count = this.counter.addAndGet(-1 * delta);
        if (debug.val)
            LOG.debug(String.format("Txn #%d - %s.run() / COUNTER: %d - %d = %d%s",
                                    this.txn_id, this.getClass().getSimpleName(),
                                    new_count+delta, delta, new_count,
                                    (trace.val ? "\n" + parameter : "")));
        
        // If this is the last result that we were waiting for, then we'll invoke
        // the unblockCallback()
        if (new_count == 0) this.unblock();
    }

    /**
     * This allows you to decrement the counter without actually needing
     * to create a ProtocolBuffer message.
     * @param delta
     * @return Returns the new value of the counter
     */
    public final int decrementCounter(int delta) {
        int new_count = this.counter.addAndGet(-1 * delta); 
        if (debug.val)
            LOG.debug(String.format("Txn #%d - Decremented %s / COUNTER: %d - %d = %s",
                                    this.txn_id, this.getClass().getSimpleName(), new_count+delta, delta, new_count));
        assert(new_count >= 0) :
            "Invalid negative " + this.getClass().getSimpleName() + " counter for txn #" + txn_id;
        if (new_count == 0) this.unblock();
        return (new_count);
    }
    
    /**
     * The implementation of the run method to process a new entry for this callback
     * This method should return how much we should decrement from the blocking counter
     * @param parameter Needs to be >=0
     * @return
     */
    protected abstract int runImpl(U parameter);
    
    // ----------------------------------------------------------------------------
    // SUCCESSFUL UNBLOCKING
    // ----------------------------------------------------------------------------
    
    /**
     * Internal method for calling the unblockCallback()
     */
    private final void unblock() {
        if (this.canceled.get() == false && (this.abortInvoked.get() == false || this.invoke_even_if_aborted)) {
            if (this.unblockInvoked.compareAndSet(false, true)) {
                if (debug.val)
                    LOG.debug(String.format("Txn #%d - Invoking %s.unblockCallback() [hashCode=%d]",
                                           this.txn_id, this.getClass().getSimpleName(), this.hashCode()));
                
                this.unblockCallback();
            } else {
                throw new RuntimeException(String.format("Txn #%d - Tried to invoke %s.unblockCallback() twice [hashCode=%d]",
                                                         this.txn_id, this.getClass().getSimpleName(), this.hashCode()));
            }
        }
    }
    
    public final boolean isUnblocked() {
        return (this.unblockInvoked.get());
    }
    
    /**
     * This method is invoked once all of the T messages are received 
     */
    protected abstract void unblockCallback();
    
    // ----------------------------------------------------------------------------
    // ABORT
    // ----------------------------------------------------------------------------
    
    /**
     * 
     */
    public final void abort(Status status) {
        // If this is the first response that told us to abort, then we'll
        // send the abort message out
        if (this.canceled.get() == false && this.abortInvoked.compareAndSet(false, true)) {
            this.abortCallback(status);
        }
    }
    
    /**
     * Returns true if this callback has invoked the abortCallback() method
     */
    public final boolean isAborted() {
        return (this.abortInvoked.get());
    }
    
    /**
     * The callback that is invoked when the first ABORT status arrives for this transaction
     * This is guaranteed to be called only once per transaction in this method 
     */
    protected abstract void abortCallback(Status status);

    // ----------------------------------------------------------------------------
    // CANCEL
    // ----------------------------------------------------------------------------
    
    /**
     * Mark this callback as canceled. No matter what happens in the future,
     * this callback will not invoke either the run or abort callbacks
     */
    public void cancel() {
        this.canceled.set(true);
    }
        
    /**
     * Returns true if this callback has been cancelled
     */
    public final boolean isCanceled() {
        return (this.canceled.get());
    }
    
    // ----------------------------------------------------------------------------
    // FINISH
    // ----------------------------------------------------------------------------

    
    @Override
    public final void finish() {
        if (debug.val) 
            LOG.debug(String.format("Txn #%d - Finishing %s [hashCode=%d]",
                                   this.txn_id, this.getClass().getSimpleName(), this.hashCode()));
        this.finishImpl();
        this.abortInvoked.set(false);
        this.unblockInvoked.set(false);
        this.canceled.set(false);
        this.orig_callback = null;
        this.txn_id = null;
    }
    
    /**
     * Special finish method for the implementing class
     */
    protected abstract void finishImpl();
    
    @Override
    public String toString() {
        return String.format("%s[Invoked=%s / Aborted=%s / Canceled=%s / Counter=%d/%d]",
                             this.getClass().getSimpleName(), 
                             this.unblockInvoked.get(),
                             this.abortInvoked.get(),
                             this.canceled.get(),
                             this.counter.get(), this.getOrigCounter()); 
    }
}
