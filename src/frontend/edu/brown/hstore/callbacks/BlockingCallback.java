package edu.brown.hstore.callbacks;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.log4j.Logger;

import com.google.protobuf.RpcCallback;

import edu.brown.hstore.Hstoreservice;
import edu.brown.logging.LoggerUtil;
import edu.brown.logging.LoggerUtil.LoggerBoolean;
import edu.brown.utils.Poolable;
import edu.brown.hstore.HStoreSite;
import edu.brown.hstore.dtxn.AbstractTransaction;

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
    private final AtomicBoolean aborted = new AtomicBoolean(false);
    
    private final AtomicBoolean invoked = new AtomicBoolean(false);
    
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

    public Long getTransactionId() {
        return (this.txn_id);
    }
    public int getCounter() {
        return this.counter.get();
    }
    public int getOrigCounter() {
        return (this.orig_counter);
    }
    public RpcCallback<T> getOrigCallback() {
        return this.orig_callback;
    }
    
    protected boolean sameTransaction(AbstractTransaction ts, Object msg, long msg_txn_id) {
        // Race condition
        Long ts_txn_id = null;
        try {
            if (ts != null) ts_txn_id = ts.getTransactionId();
        } catch (NullPointerException ex) {
            // Ignore
        } finally {
            // IMPORTANT: If the LocalTransaction handle is null, then that means we are getting
            // this message well after we have already cleaned up the transaction. Since these objects
            // are pooled, it could be reused. So that means we will just ignore it.
            // This may make it difficult to debug, but hopefully we'll be ok.
            if (ts_txn_id == null) {
                if (debug.get()) LOG.warn(String.format("Ignoring old %s for defunct txn #%d",
                                                         msg.getClass().getSimpleName(), msg_txn_id));
                return (false);
            }
        }
        // If we get a response that matches our original txn but the LocalTransaction handle 
        // has changed, then we need to will just ignore it
        if (msg_txn_id != ts_txn_id.longValue()) {
            if (debug.get()) LOG.debug(String.format("Ignoring %s for a different transaction #%d [origTxn=#%d]",
                                                     msg.getClass().getSimpleName(), msg_txn_id, this.getTransactionId()));
            return (false);
        }
        return (true);
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
        if ((this.aborted.get() == false || this.invoke_even_if_aborted) && new_count == 0) {
            this.unblock();
        }
    }
    
    /**
     * This allows you to decrement the counter without actually needing
     * to create a ProtocolBuffer message.
     */
    public void decrementCounter(int ctr) {
        if (debug.get())
            LOG.debug(String.format("Txn #%d - Decrementing %s counter by %d",
                                    txn_id, this.getClass().getSimpleName(), ctr));
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
    
    
    private void unblock() {
        if (debug.get())
            LOG.debug(String.format("Txn #%d - Invoking %s.unblockCallback()",
                                    this.txn_id, this.getClass().getSimpleName()));
        if (this.invoked.compareAndSet(false, true)) {
            this.unblockCallback();
        } else {
            assert(false) :
                String.format("Txn #%d - Tried to invoke %s.unblockCallback() twice!",
                              this.txn_id, this.getClass().getSimpleName());
        }
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
    public final void abort(Hstoreservice.Status status) {
        // If this is the first response that told us to abort, then we'll
        // send the abort message out 
        if (this.aborted.compareAndSet(false, true)) {
            this.abortCallback(status);
        }
    }
    
    public boolean isAborted() {
        return (this.aborted.get());
    }
    
    /**
     * 
     */
    protected abstract void abortCallback(Hstoreservice.Status status);

    // ----------------------------------------------------------------------------
    // FINISH
    // ----------------------------------------------------------------------------

    
    @Override
    public final void finish() {
        if (this instanceof TransactionPrepareCallback || this instanceof TransactionFinishCallback)
            assert(this.counter.get() == 0) :
                String.format("Trying to finish %s for txn #%d before it was properly finished [counter=%d]",
                              this.getClass().getSimpleName(), this.txn_id, this.counter.get());
        if (debug.get()) LOG.debug(String.format("Txn #%d - Finishing %s",
                                                 this.txn_id, this.getClass().getSimpleName()));
        
        this.aborted.set(false);
        this.invoked.set(false);
        this.orig_callback = null;
        this.txn_id = null;
        this.finishImpl();
    }
    
    /**
     * 
     */
    protected abstract void finishImpl();
    

}
