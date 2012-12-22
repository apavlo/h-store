package edu.brown.hstore.callbacks;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.log4j.Logger;

import edu.brown.hstore.HStoreSite;
import edu.brown.hstore.Hstoreservice.Status;
import edu.brown.hstore.conf.HStoreConf;
import edu.brown.hstore.txns.AbstractTransaction;
import edu.brown.logging.LoggerUtil;
import edu.brown.logging.LoggerUtil.LoggerBoolean;
import edu.brown.pools.Poolable;
import edu.brown.utils.PartitionSet;

public abstract class PartitionCountingCallback<X extends AbstractTransaction> implements Poolable {
    private static final Logger LOG = Logger.getLogger(PartitionCountingCallback.class);
    private static final LoggerBoolean debug = new LoggerBoolean(LOG.isDebugEnabled());
    private static final LoggerBoolean trace = new LoggerBoolean(LOG.isTraceEnabled());
    static {
        LoggerUtil.attachObserver(LOG, debug, trace);
    }
    
    // ----------------------------------------------------------------------------
    // INTERNAL DATA MEMBERS
    // ----------------------------------------------------------------------------
    
    protected final HStoreSite hstore_site;
    protected final HStoreConf hstore_conf;
    private final AtomicInteger counter = new AtomicInteger(0);
    private final PartitionSet partitions = new PartitionSet();
    private final PartitionSet recievedPartitions = new PartitionSet();
    
    /**
     * The current transaction handle that this callback is assigned to
     */
    protected X ts;
    
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
     * This flag is set to true if the callback has been cancelled
     */
    private final AtomicBoolean canceled = new AtomicBoolean(false);

    // We retain the original parameters of the last init() for debugging
    private Long orig_txn_id = null;
    private int orig_counter;
    
    // ----------------------------------------------------------------------------
    // CONSTRUCTOR
    // ----------------------------------------------------------------------------
    
    /**
     * Constructor
     * If invoke_even_if_aborted set to true, then this callback will still execute
     * the unblockCallback() method after all the responses have arrived. 
     * @param invoke_even_if_aborted  
     */
    protected PartitionCountingCallback(HStoreSite hstore_site) {
        this.hstore_site = hstore_site;
        this.hstore_conf = hstore_site.getHStoreConf();
    }
    
    // ----------------------------------------------------------------------------
    // INITIALIZATION
    // ----------------------------------------------------------------------------
    
    /**
     * Initialize the BlockingCallback's counter and transaction info
     * @param ts - The transaction handle for this callback
     * @param partitions - The partitions that we expected to get notifications for
     */
    protected void init(X ts, PartitionSet partitions) {
        if (debug.get()) LOG.debug(String.format("%s - Initialized new %s with partitions %s counter = %d hashCode=%d]",
                                   ts, this.getClass().getSimpleName(), partitions, this.hashCode()));
        int counter_val = partitions.size();
        this.orig_counter = counter_val;
        this.counter.set(counter_val);
        this.partitions.addAll(partitions);
        this.ts = ts;
        this.orig_txn_id = this.ts.getTransactionId();
    }
    
    @Override
    public boolean isInitialized() {
        return (this.ts != null);
    }
    
    // ----------------------------------------------------------------------------
    // UTILITY METHODS
    // ----------------------------------------------------------------------------

    protected final X getTransaction() {
        return (this.ts);
    }
    
    
    // ----------------------------------------------------------------------------
    // RUN
    // ----------------------------------------------------------------------------
    
    public final void run(int partition) {
        int delta = this.runImpl(partition);
        int new_count = this.counter.addAndGet(-1 * delta);
        if (debug.get()) LOG.debug(String.format("%s - %s.run() / COUNTER: %d - %d = %d%s",
                                   this.ts, this.getClass().getSimpleName(),
                                   new_count+delta, delta, new_count,
                                   (trace.get() ? "\n" + partition : "")));
        
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
        if (debug.get()) LOG.debug(String.format("%s - Decremented %s / COUNTER: %d - %d = %s",
                                   this.ts, this.getClass().getSimpleName(), new_count+delta, delta, new_count));
        assert(new_count >= 0) :
            "Invalid negative " + this.getClass().getSimpleName() + " counter for " + this.ts;
        if (new_count == 0) this.unblock();
        return (new_count);
    }
    
    /**
     * The implementation of the run method to process a new entry for this callback
     * This method should return how much we should decrement from the blocking counter
     * @param parameter Needs to be >=0
     * @return
     */
    protected abstract int runImpl(int partition);
    
    // ----------------------------------------------------------------------------
    // SUCCESSFUL UNBLOCKING
    // ----------------------------------------------------------------------------
    
    /**
     * Internal method for calling the unblockCallback()
     */
    private final void unblock() {
        if (this.canceled.get() == false && this.abortInvoked.get() == false) {
            if (this.unblockInvoked.compareAndSet(false, true)) {
                if (debug.get()) LOG.debug(String.format("%s - Invoking %s.unblockCallback() [hashCode=%d]",
                                           this.ts, this.getClass().getSimpleName(), this.hashCode()));
                this.unblockCallback();
                this.unblockFinished = true;
            } else {
                String msg = String.format("%s - Tried to invoke %s.unblockCallback() twice [hashCode=%d]",
                                           this.ts, this.getClass().getSimpleName(), this.hashCode());
                throw new RuntimeException(msg);
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
        assert(this.ts != null) :
            String.format("Null transaction handle for txn #%s in %s [counter=%d/%d]",
                          this.orig_txn_id, this.getClass().getSimpleName(),
                          this.counter.get(), this.orig_counter);
        assert(this.ts.isInitialized()) :
            String.format("Uninitialized transaction handle for txn #%s in %s [lastTxn=%s / origCounter=%d/%d]",
                          this.orig_txn_id, this.getClass().getSimpleName(),
                          this.counter.get(), this.orig_counter);
        
        // If this is the first response that told us to abort, then we'll
        // send the abort message out
        if (this.canceled.get() == false && this.abortInvoked.compareAndSet(false, true)) {
            this.abortCallback(status);
            this.abortFinished = true;
            
            // If we abort, then we have to send out an ABORT to
            // all of the partitions that we originally sent INIT requests too
            // Note that we do this *even* if we haven't heard back from the remote
            // HStoreSite that they've acknowledged our transaction
            // We don't care when we get the response for this
//            if (finish && this.ts.isPredictSinglePartition() == false) {
//                if (this.ts instanceof LocalTransaction) {
//                    this.finishTransaction(status);
//                } else {
//                    // FIXME
//                }
//            }
            
            this.hstore_site.queueDeleteTransaction(this.ts.getTransactionId(), status);
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
        if (debug.get()) LOG.debug(String.format("%s - Finishing %s [hashCode=%d]",
                                   this.ts, this.getClass().getSimpleName(), this.hashCode()));
        this.abortInvoked.lazySet(false);
        this.unblockInvoked.lazySet(false);
        this.canceled.lazySet(false);
        this.finishImpl();
        this.partitions.clear();
        this.recievedPartitions.clear();
        this.ts = null;
    }
    
    /**
     * Special finish method for the implementing class
     */
    protected abstract void finishImpl();
    
    // ----------------------------------------------------------------------------
    // CALLBACK CHECKS
    // ----------------------------------------------------------------------------
    
    /**
     * Returns true if either the unblock or abort callbacks have been invoked
     * and have finished their processing
     */
    public final boolean allCallbacksFinished() {
        if (this.isCanceled() == false && this.isInitialized()) {
            if (this.counter.get() != 0) return (false);
            return ((this.isUnblocked() && this.unblockFinished) || (this.isAborted() && this.abortFinished));
        }
        return (true);
    }
}
