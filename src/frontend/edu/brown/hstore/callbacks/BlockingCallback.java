package edu.brown.hstore.callbacks;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.log4j.Logger;

import com.google.protobuf.RpcCallback;

import edu.brown.hstore.Hstoreservice.Status;
import edu.brown.logging.LoggerUtil;
import edu.brown.logging.LoggerUtil.LoggerBoolean;
import edu.brown.utils.Poolable;
import edu.brown.hstore.HStoreSite;

/**
 * 
 * @param <T> The message type of the original RpcCallback
 * @param <U> The message type that we will accumulate before invoking the original RpcCallback
 */
public abstract class BlockingCallback<T, U> implements RpcCallback<U>, Poolable {
    private static final Logger LOG = Logger.getLogger(BlockingCallback.class);
    private static final LoggerBoolean debug = new LoggerBoolean(LOG.isDebugEnabled());
    private static final LoggerBoolean trace = new LoggerBoolean(LOG.isTraceEnabled());
    static {
        LoggerUtil.attachObserver(LOG, debug, trace);
    }
    
    protected final HStoreSite hstore_site;
    private Long txn_id = null;
    private final AtomicInteger counter = new AtomicInteger(0);
    private int orig_counter;
    private RpcCallback<T> orig_callback;

    /**
     * We'll flip this flag if one of our partitions replies with an
     * unexpected abort. This ensures that we only send out the ABORT
     * to all the HStoreSites once. 
     */
    private final AtomicBoolean abortInvoked = new AtomicBoolean(false);
    
    /**
     * This flag is set to true after the abortCallback() invocation is finished
     * This prevents somebody from checking whether we have invoked the abort callback
     * but are still in the middle of processing it.
     */
    private boolean abortFinished = false;
    
    /**
     * This flag is set to true when the unblockCallback() is invoked
     */
    private final AtomicBoolean unblockInvoked = new AtomicBoolean(false);
    
    /**
     * This flag is set to true after the unblockCallback() invocation is finished
     * This prevents somebody from checking whether we have invoked the unblock callback
     * but are still in the middle of processing it.
     */
    private boolean unblockFinished = false;
    
    /**
     * If set to true, then this callback will still invoke unblockCallback()
     * once all of the messages arrive
     */
    private final boolean invoke_even_if_aborted;
    
    /**
     * Default Constructor
     * @param invoke_even_if_aborted TODO
     */
    protected BlockingCallback(HStoreSite hstore_site, boolean invoke_even_if_aborted) {
        this.hstore_site = hstore_site;
        this.invoke_even_if_aborted = invoke_even_if_aborted;
    }
    
    /**
     * Initialize the BlockingCallback's counter and transaction info
     * @param txn_id
     * @param counter_val
     * @param orig_callback
     */
    protected void init(Long txn_id, int counter_val, RpcCallback<T> orig_callback) {
        if (debug.get()) LOG.debug(String.format("Txn #%d - Initialized new %s with counter = %d",
                                                 txn_id, this.getClass().getSimpleName(), counter_val));
        this.orig_counter = counter_val;
        this.counter.set(counter_val);
        this.orig_callback = orig_callback;
        this.txn_id = txn_id;
    }
    
    @Override
    public boolean isInitialized() {
        return (this.orig_callback != null);
    }

    protected final Long getTransactionId() {
        return (this.txn_id);
    }
    public final int getCounter() {
        return this.counter.get();
    }
    protected final int getOrigCounter() {
        return (this.orig_counter);
    }
    protected final RpcCallback<T> getOrigCallback() {
        return this.orig_callback;
    }
    
    /**
     * Returns true if either the unblock or abort callbacks have been invoked
     * and have finished their processing
     */
    public final boolean allCallbacksFinished() {
        return (this.isUnblockCallbackFinished() && this.isAbortCallbackFinished());
    }
    
    // ----------------------------------------------------------------------------
    // RUN
    // ----------------------------------------------------------------------------
    
    @Override
    public final void run(U parameter) {
        int delta = this.runImpl(parameter);
        int new_count = this.counter.addAndGet(-1 * delta);
        if (debug.get())
            LOG.debug(String.format("Txn #%d - %s.run() / COUNTER: %d - %d = %d\n%s",
                                    this.txn_id, this.getClass().getSimpleName(),
                                    new_count+delta, delta, new_count, parameter));
        
        // If this is the last result that we were waiting for, then we'll invoke
        // the unblockCallback()
        if ((this.abortInvoked.get() == false || this.invoke_even_if_aborted) && new_count == 0) {
            this.unblock();
        }
    }
    
    /**
     * This allows you to decrement the counter without actually needing
     * to create a ProtocolBuffer message.
     */
    public final void decrementCounter(int ctr) {
        if (debug.get())
            LOG.debug(String.format("Txn #%d - Decrementing %s counter by %d",
                                    this.txn_id, this.getClass().getSimpleName(), ctr));
        if (this.counter.addAndGet(-1 * ctr) == 0) {
            this.unblock();
        }
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
        if (debug.get())
            LOG.debug(String.format("Txn #%d - Invoking %s.unblockCallback()",
                                    this.txn_id, this.getClass().getSimpleName()));
        
        if (this.unblockInvoked.compareAndSet(false, true)) {
            this.unblockCallback();
            this.unblockFinished = true;
        } else {
            throw new RuntimeException(String.format("Txn #%d - Tried to invoke %s.unblockCallback() twice!",
                                                     this.txn_id, this.getClass().getSimpleName()));
        }
    }
    
    /**
     * Returns true if this callback has been aborted and we have successfully
     * completed the unblockCallback() method. This means that it is safe
     * to go ahead and clean-up this callback.
     * @return
     */
    public final boolean isUnblockCallbackFinished() {
        return (this.unblockInvoked.get() && this.unblockFinished);
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
        if (this.abortInvoked.compareAndSet(false, true)) {
            this.abortCallback(status);
            this.unblockFinished = true;
        }
    }
    
    /**
     * Returns true if this callback has invoked the abortCallback() method
     */
    protected final boolean isAborted() {
        return (this.abortInvoked.get());
    }
    
    /**
     * Returns true if this callback has been aborted and we have succesfully
     * completed the abortCallback() method. This means that it is safe
     * to go ahead and clean-up this callback.
     * @return
     */
    public final boolean isAbortCallbackFinished() {
        return (this.abortInvoked.get() && this.abortFinished);
    }
    
    /**
     * The callback that is invoked when the first ABORT status arrives for this transaction
     * This is guaranteed to be called only once per transaction in this method 
     */
    protected abstract void abortCallback(Status status);

    // ----------------------------------------------------------------------------
    // FINISH
    // ----------------------------------------------------------------------------

    
    @Override
    public final void finish() {
        if (debug.get()) LOG.debug(String.format("Txn #%d - Finishing %s",
                                                 this.txn_id, this.getClass().getSimpleName()));
        
        this.abortInvoked.set(false);
        this.unblockInvoked.set(false);
        this.unblockFinished = false;
        this.orig_callback = null;
        this.txn_id = null;
        this.finishImpl();
    }
    
    /**
     * Special finish method for the implementing class
     */
    protected abstract void finishImpl();
    
    
    @Override
    public String toString() {
        return String.format("%s[Counter=%d]", super.toString(), this.counter.get()); 
    }
}
