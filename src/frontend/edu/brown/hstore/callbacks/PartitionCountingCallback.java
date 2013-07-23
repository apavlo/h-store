package edu.brown.hstore.callbacks;

import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.log4j.Logger;

import edu.brown.hstore.HStoreSite;
import edu.brown.hstore.Hstoreservice.Status;
import edu.brown.hstore.conf.HStoreConf;
import edu.brown.hstore.txns.AbstractTransaction;
import edu.brown.hstore.txns.LocalTransaction;
import edu.brown.logging.LoggerUtil;
import edu.brown.logging.LoggerUtil.LoggerBoolean;
import edu.brown.utils.ClassUtil;
import edu.brown.utils.PartitionSet;
import edu.brown.utils.StringUtil;

public abstract class PartitionCountingCallback<X extends AbstractTransaction> implements TransactionCallback {
    private static final Logger LOG = Logger.getLogger(PartitionCountingCallback.class);
    private static final LoggerBoolean debug = new LoggerBoolean();
    private static final LoggerBoolean trace = new LoggerBoolean();
    static {
        LoggerUtil.attachObserver(LOG, debug, trace);
    }
    
    // ----------------------------------------------------------------------------
    // INTERNAL DATA MEMBERS
    // ----------------------------------------------------------------------------
    
    protected final HStoreSite hstore_site;
    protected final HStoreConf hstore_conf;
    private int counter = 0;
    private final PartitionSet partitions = new PartitionSet();
    private final PartitionSet receivedPartitions = new PartitionSet();
    
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
    private boolean canceled = false;

    // We retain the original parameters of the last init()
    private Long orig_txn_id = null;
    private int orig_counter;
    
    private final PartitionSet runPartitions = new PartitionSet();
    private final PartitionSet abortPartitions = new PartitionSet();
    
    
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
        if (debug.val)
            LOG.debug(String.format("%s - Initialized %s with partitions %s [counter=%d, hashCode=%d]",
                      ts, this.getClass().getSimpleName(), partitions, partitions.size(), this.hashCode()));
        int counter_val = partitions.size();
        this.orig_counter = counter_val;
        this.counter = counter_val;
        this.partitions.addAll(partitions);
        this.ts = ts;
        this.orig_txn_id = this.ts.getTransactionId();
    }
    
    @Override
    public boolean isInitialized() {
        return (this.ts != null);
    }
    
    // ----------------------------------------------------------------------------
    // FINISH
    // ----------------------------------------------------------------------------
    
    @Override
    public final void finish() {
        if (debug.val)
            LOG.debug(String.format("%s - Finishing %s [hashCode=%d]",
                      this.ts, this.getClass().getSimpleName(), this.hashCode()));
        this.abortInvoked.lazySet(false);
        this.abortFinished = false;
        this.unblockInvoked.lazySet(false);
        this.unblockFinished = false;
        this.canceled = false;
        this.partitions.clear();
        this.receivedPartitions.clear();
        this.counter = 0;
        this.orig_counter = 0;
        this.ts = null;
        
        this.runPartitions.clear();
        this.abortPartitions.clear();
    }

    // ----------------------------------------------------------------------------
    // UTILITY METHODS
    // ----------------------------------------------------------------------------

    /**
     * Returns true if either the unblock or abort callbacks have been invoked
     * and have finished their processing
     */
    public synchronized final boolean allCallbacksFinished() {
        if (this.canceled == false && this.orig_counter > 0) {
            if (this.counter != 0) return (false);
            if (this.unblockFinished || this.abortFinished) return (true);
            return ((this.unblockInvoked.get() && this.unblockFinished) ||
                    (this.abortInvoked.get() && this.abortFinished));
        }
        return (true);
    }
    
    protected final X getTransaction() {
        return (this.ts);
    }
    
    /**
     * Return the current state of this callback's internal counter
     */
    protected synchronized final int getCounter() {
        return (this.counter);
    }
    
    /**
     * Return all of the partitions involved in this callback.
     */
    public PartitionSet getPartitions() {
        return (this.partitions);
    }
    
    /**
     * Return all of the partitions that this callback has received.
     */
    public PartitionSet getReceivedPartitions() {
       return (this.receivedPartitions); 
    }
    
    /**
     * Tell the HStoreCoordinator to invoke the TransactionFinish process
     * @param status
     */
    protected final void finishTransaction(Status status) {
        assert(this.ts != null) :
            "Null transaction handle for txn #" + this.orig_txn_id;
        if (debug.val)
            LOG.debug(String.format("%s - Invoking TransactionFinish protocol from %s [status=%s]\n%s",
                      this.ts, this.getClass().getSimpleName(), status,
                      StringUtil.join("\n", ClassUtil.getStackTrace())));
        
        // Let everybody know that the party is over!
        if (this.ts instanceof LocalTransaction) {
            LocalTransaction local_ts = (LocalTransaction)this.ts;
            LocalFinishCallback callback = ((LocalTransaction)this.ts).getFinishCallback();
            callback.init(local_ts, status);
            this.hstore_site.getCoordinator().transactionFinish(local_ts, status, callback);
        }
    }
    
    protected final Long getOrigTransactionId() {
        return (this.orig_txn_id);
    }
    protected final int getOrigCounter() {
        return (this.orig_counter);
    }
    
    // ----------------------------------------------------------------------------
    // RUN
    // ----------------------------------------------------------------------------
    
    public void run(int partition) {
        // If this is the last result that we were waiting for, then we'll invoke
        // the unblockCallback()
        if (this.decrementCounter(partition)) {
            this.unblock();
            this.runPartitions.add(partition);
        }
    }
    
    /**
     * This allows you to decrement the counter without actually needing
     * to create a ProtocolBuffer message.
     * @param delta
     * @return Returns the new value of the counter
     */
    public synchronized final boolean decrementCounter(int partition) {
        assert(this.partitions.contains(partition)) :
            String.format("Trying to decrement for unexpected partition %d for %s [expected=%s]",
                          partition, this.ts, this.partitions);
        
        if (this.receivedPartitions.contains(partition) == false) {
            this.counter--;
            this.receivedPartitions.add(partition);
            if (debug.val)
                LOG.debug(String.format("%s - %s.decrementCounter(partition=%d) / " +
                		  "COUNTER: %d - %d = %d [origCtr=%d]%s",
                          this.ts, this.getClass().getSimpleName(), partition,
                          this.counter+1, 1, this.counter, this.orig_counter,
                          (trace.val ? "\n" + partition : "")));
            assert(this.counter >= 0 && (this.counter == (this.partitions.size() - this.receivedPartitions.size()))) :
                String.format("Invalid %s counter after decrementing for %s at partition %d\n%s",
                              this.getClass().getSimpleName(),
                              this.ts, partition, this);
            return (this.counter == 0);
        }
        return (false);
    }
    
    // ----------------------------------------------------------------------------
    // SUCCESSFUL UNBLOCKING
    // ----------------------------------------------------------------------------
    
    /**
     * Internal method for calling the unblockCallback()
     */
    private final void unblock() {
        if (this.canceled == false && this.abortInvoked.get() == false) {
            if (this.unblockInvoked.compareAndSet(false, true)) {
                if (debug.val)
                    LOG.debug(String.format("%s - Invoking %s.unblockCallback() [hashCode=%d]",
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
    
    @Override
    public final void abort(int partition, Status status) {
        assert(this.ts != null) :
            String.format("Null transaction handle for txn #%s in %s [counter=%d/%d]\n%s",
                          this.orig_txn_id, this.getClass().getSimpleName(),
                          this.counter, this.orig_counter,
                          (this.orig_txn_id != null ? this.hstore_site.getTransaction(this.orig_txn_id) : null));
        assert(this.ts.isInitialized()) :
            String.format("Uninitialized transaction handle for txn #%s in %s [lastTxn=%s / origCounter=%d/%d]",
                          this.orig_txn_id, this.getClass().getSimpleName(),
                          this.counter, this.orig_counter);
        
        // Always attempt to add the partition
        this.decrementCounter(partition);
        this.abortPartitions.add(partition);
        
        // If this is the first response that told us to abort, then we'll
        // send the abort message out
        if (this.canceled == false && this.abortInvoked.compareAndSet(false, true)) {
            if (this.unblockInvoked.get()) {
                LOG.warn(String.format("%s - Trying to call %s.abortCallback() after having been unblocked!\n%s",
                         this.ts, this.getClass().getSimpleName(), this));
                return;
            }
            
            if (debug.val)
                LOG.debug(String.format("%s - Invoking %s.abortCallback() [partition=%d, status=%s]",
                          this.ts, this.getClass().getSimpleName(), partition, status));
            try {
                this.abortCallback(partition, status);
            } catch (RuntimeException ex) {
                String msg = String.format("Failed to invoke %s.abortCallback() for %s [partition=%d, status=%s]",
                             this.getClass().getSimpleName(), this.ts,
                             partition, status);
                LOG.error(msg, ex);
                throw ex;
            }
            
            // If we abort, then we have to send out an ABORT to
            // all of the partitions that we originally sent INIT requests too
            // Note that we do this *even* if we haven't heard back from the remote
            // HStoreSite that they've acknowledged our transaction
            // We don't care when we get the response for this
            if (this.ts.isPredictSinglePartition() == false) {
                if (this.ts instanceof LocalTransaction) {
                    this.finishTransaction(status);
                } else {
                    // FIXME
                }
            } else {
                this.hstore_site.queueDeleteTransaction(this.ts.getTransactionId(), status); 
            }
            this.abortFinished = true;
        }
    }
    
    @Override
    public final boolean isAborted() {
        return (this.abortInvoked.get());
    }
    
    /**
     * The callback that is invoked when the first ABORT status arrives for this transaction
     * This is guaranteed to be called only once per transaction in this method 
     */
    protected abstract void abortCallback(int partition, Status status);

    // ----------------------------------------------------------------------------
    // CANCEL
    // ----------------------------------------------------------------------------
    
    @Override
    public void cancel() {
        this.canceled = true;
    }
    @Override
    public final boolean isCanceled() {
        return (this.canceled);
    }
    
    // ----------------------------------------------------------------------------
    // DEBUG METHODS
    // ----------------------------------------------------------------------------
    
    @Override
    public String toString() {
        String ret = String.format("[Invoked=%s/%s, Aborted=%s/%s, Canceled=%s, Counter=%d/%d] => Deletable=%s",
                                   this.unblockInvoked.get(), this.unblockFinished,
                                   this.abortInvoked.get(), this.abortFinished,
                                   this.canceled,
                                   this.counter, this.orig_counter,
                                   this.allCallbacksFinished());
//        if (debug.val) {
            ret += String.format("\nReceivedPartitions=%s / AllPartitions=%s",
                                 this.receivedPartitions, this.partitions);
            ret += String.format("\nRunPartitions=%s / AbortPartitions=%s",
                    this.runPartitions, this.abortPartitions);
//        }
        return (ret);
    }
}
