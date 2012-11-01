package edu.brown.hstore.callbacks;

import org.apache.log4j.Logger;

import com.google.protobuf.RpcCallback;

import edu.brown.hstore.HStoreSite;
import edu.brown.hstore.Hstoreservice.Status;
import edu.brown.hstore.txns.LocalTransaction;
import edu.brown.logging.LoggerUtil;
import edu.brown.logging.LoggerUtil.LoggerBoolean;

/**
 * Special BlockingCallback wrapper for transactions. This class has utility methods for
 * identifying when it is safe to delete a transaction handle from our local HStoreSite. 
 * @author pavlo
 * @param <T> The message type of the original RpcCallback
 * @param <U> The message type that we will accumulate before invoking the original RpcCallback
 */
public abstract class AbstractTransactionCallback<T, U> extends BlockingRpcCallback<T, U> {
    private static final Logger LOG = Logger.getLogger(AbstractTransactionCallback.class);
    private static final LoggerBoolean debug = new LoggerBoolean(LOG.isDebugEnabled());
    private static final LoggerBoolean trace = new LoggerBoolean(LOG.isTraceEnabled());
    static {
        LoggerUtil.attachObserver(LOG, debug, trace);
    }
    
    protected LocalTransaction ts;
    private Status finishStatus;
    
    
    /**
     * This flag is set to true after the unblockCallback() invocation is finished
     * This prevents somebody from checking whether we have invoked the unblock callback
     * but are still in the middle of processing it.
     */
    private boolean unblockFinished = false;
    
    /**
     * This flag is set to true after the abortCallback() invocation is finished
     * This prevents somebody from checking whether we have invoked the abort callback
     * but are still in the middle of processing it.
     */
    private boolean abortFinished = false;
    
    /**
     * Constructor
     * @param hstore_site
     */
    protected AbstractTransactionCallback(HStoreSite hstore_site) {
        super(hstore_site, false);
    }
    
    protected void init(LocalTransaction ts, int counter_val, RpcCallback<T> orig_callback) {
        assert(ts != null) : "Unexpected null LocalTransaction handle";
        if (debug.get()) LOG.debug(ts + " - Initializing new " + this.getClass().getSimpleName());
        super.init(ts.getTransactionId(), counter_val, orig_callback);
        this.ts = ts;
    }

    @Override
    protected void finishImpl() {
        this.ts = null;
        this.finishStatus = null;
        this.unblockFinished = false;
        this.abortFinished = false;
    }

    @Override
    public final boolean isInitialized() {
        return (this.ts != null);
    }
    
    @Override
    protected final void unblockCallback() {
        assert(this.isUnblocked());
        assert(this.ts != null) :
            String.format("Unexpected null transaction handle for txn #%s in %s [lastTxn=%s]",
                          this.getTransactionId(), this.getClass().getSimpleName(), this.lastTxnId);
        assert(this.ts.isInitialized()) :
            String.format("Unexpected uninitalized transaction handle for txn #%s in %s [lastTxn=%s]",
                          this.getTransactionId(), this.getClass().getSimpleName(), this.lastTxnId);
        
        boolean delete = true;
        if (this.isAborted() == false) {
            delete = this.unblockTransactionCallback();
        }
        this.unblockFinished = true;
        if (delete) this.hstore_site.queueDeleteTransaction(this.txn_id, this.finishStatus);
    }
    
    @Override
    protected final void abortCallback(Status status) {
        assert(this.isAborted());
        this.finishStatus = status;
        
        // We can't abort if we've already invoked the regular callback
        boolean finish = false;
        if (this.isUnblocked() == false) {
            finish = this.abortTransactionCallback(status);
        }
        
        // If we abort, then we have to send out an ABORT to
        // all of the partitions that we originally sent INIT requests too
        // Note that we do this *even* if we haven't heard back from the remote
        // HStoreSite that they've acknowledged our transaction
        // We don't care when we get the response for this
        if (finish) {
            this.finishTransaction(status);
        }
        this.abortFinished = true;
        this.hstore_site.queueDeleteTransaction(this.txn_id, status);
    }

    /**
     * Transaction unblocking callback implementation
     * If this returns true, then we will invoke deleteTransaction()
     * @return
     */
    protected abstract boolean unblockTransactionCallback();
    
    /**
     * Transaction abort callback implementation
     * If this returns true, then we will invoke finishTransaction() 
     * @return
     */
    protected abstract boolean abortTransactionCallback(Status status);
    
    // ----------------------------------------------------------------------------
    // CALLBACK CHECKS
    // ----------------------------------------------------------------------------
    
    /**
     * Returns true if either the unblock or abort callbacks have been invoked
     * and have finished their processing
     */
    public final boolean allCallbacksFinished() {
        if (this.isInitialized()) {
            if (this.getCounter() != 0) return (false);
            return ((this.isUnblocked() && this.unblockFinished) || (this.isAborted() && this.abortFinished));
        }
        return (true);
    }
    
    // ----------------------------------------------------------------------------
    // INTERNAL UTILITY METHODS
    // ----------------------------------------------------------------------------
    
    protected void setFinishStatus(Status status) {
        this.finishStatus = status;
    }
    
    /**
     * Checks whether a transaction is ready to be deleted
     * This is thread-safe
     * @param status
     */
//    private final void deleteTransaction(Status status) {
//        if (this.ts.isDeletable()) {
//            if (this.txn_profiling) ts.profiler.stopPostFinish();
////            if (debug.get()) 
//                LOG.info(String.format("%s - Deleting from %s [status=%s]",
//                                                     this.ts, this.getClass().getSimpleName(), status));
//            this.hstore_site.deleteTransaction(this.getTransactionId(), status);
//        } else { // if (debug.get()) {
//            LOG.warn(String.format("%s - Not deleting from %s [status=%s]\n%s",
//                                   this.ts, this.getClass().getSimpleName(), status, this.ts.debug()));
//        }
//    }
    
    /**
     * Tell the HStoreCoordinator to invoke the TransactionFinish process
     * @param status
     */
    protected final void finishTransaction(Status status) {
        assert(this.ts != null) :
            "Unexpected null transaction handle for txn #" + this.getTransactionId();
        if (debug.get()) LOG.debug(String.format("%s - Invoking TransactionFinish protocol from %s [status=%s]",
                                                 this.ts, this.getClass().getSimpleName(), status));
        
        // Let everybody know that the party is over!
        TransactionFinishCallback finish_callback = this.ts.initTransactionFinishCallback(status);
        this.hstore_site.getCoordinator().transactionFinish(this.ts, status, finish_callback);
    }
    
    @Deprecated
    protected boolean sameTransaction(Object msg, long msg_txn_id) {
        // Race condition
        Long ts_txn_id = null;
        try {
            if (this.ts != null) ts_txn_id = this.ts.getTransactionId();
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
    
    @Override
    public String toString() {
        return String.format("%s / Deletable=%s",
                             super.toString(), this.allCallbacksFinished());
    }
}
