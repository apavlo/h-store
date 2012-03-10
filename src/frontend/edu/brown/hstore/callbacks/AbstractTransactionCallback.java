package edu.brown.hstore.callbacks;

import org.apache.log4j.Logger;

import com.google.protobuf.RpcCallback;

import edu.brown.hstore.HStoreSite;
import edu.brown.hstore.Hstoreservice.Status;
import edu.brown.hstore.dtxn.LocalTransaction;
import edu.brown.logging.LoggerUtil;
import edu.brown.logging.LoggerUtil.LoggerBoolean;

/**
 * Special BlockingCallback wrapper for transactions. This class has utility methods for
 * identifying when it is safe to delete a transaction handle from our local HStoreSite. 
 * @author pavlo
 * @param <T> The message type of the original RpcCallback
 * @param <U> The message type that we will accumulate before invoking the original RpcCallback
 */
public abstract class AbstractTransactionCallback<T, U> extends BlockingCallback<T, U> {
    private static final Logger LOG = Logger.getLogger(AbstractTransactionCallback.class);
    private static final LoggerBoolean debug = new LoggerBoolean(LOG.isDebugEnabled());
    private static final LoggerBoolean trace = new LoggerBoolean(LOG.isTraceEnabled());
    static {
        LoggerUtil.attachObserver(LOG, debug, trace);
    }

    
    protected final boolean txn_profiling;
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
        super(hstore_site, true);
        this.txn_profiling = hstore_site.getHStoreConf().site.txn_profiling;
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
        assert(this.ts != null) :
            "Unexpected null transaction handle for txn #" + this.getTransactionId();
        assert(this.ts.isInitialized()) :
            "Unexpected uninitalized transaction handle for txn #" + this.getTransactionId();
        
        boolean delete = true;
        if (this.isAborted() == false) {
            delete = this.unblockTransactionCallback();
        }
        this.unblockFinished = true;
        if (delete) this.deleteTransaction(this.finishStatus);
    }
    
    @Override
    protected final void abortCallback(Status status) {
        this.finishStatus = status;
        boolean finish = this.abortTransactionCallback(status);
        
        // If we abort, then we have to send out an ABORT to
        // all of the partitions that we originally sent INIT requests too
        // Note that we do this *even* if we haven't heard back from the remote
        // HStoreSite that they've acknowledged our transaction
        // We don't care when we get the response for this
        if (finish) {
            if (this.txn_profiling) {
                this.ts.profiler.stopPostPrepare();
                this.ts.profiler.startPostFinish();
            }
            this.finishTransaction(status);
        }
        this.abortFinished = true;
        this.deleteTransaction(status);
    }
    
    /**
     * 
     */
    protected abstract boolean unblockTransactionCallback();
    protected abstract boolean abortTransactionCallback(Status status);
    
    // ----------------------------------------------------------------------------
    // CALLBACK CHECKS
    // ----------------------------------------------------------------------------
    
    /**
     * Returns true if either the unblock or abort callbacks have been invoked
     * and have finished their processing
     */
    public final boolean allCallbacksFinished() {
        return ((this.isUnblocked() && this.unblockFinished) ||
                (this.isAborted() && this.abortFinished));
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
    private final void deleteTransaction(Status status) {
        if (this.ts.isDeletable()) {
            if (this.txn_profiling) ts.profiler.stopPostFinish();
//            if (debug.get()) 
                LOG.debug(String.format("%s - Deleting from %s [status=%s]",
                                                     this.ts, this.getClass().getSimpleName(), status));
            hstore_site.deleteTransaction(this.getTransactionId(), status);
        } else { // if (debug.get()) {
            LOG.info(String.format("%s - Not deleting from %s [status=%s]\n%s",
                                   this.ts, this.getClass().getSimpleName(), status, this.ts.debug()));
        }
    }
    
    /**
     * Tell the HStoreCoordinator to invoke the TransactionFinish process
     * @param status
     */
    protected final void finishTransaction(Status status) {
        // Let everybody know that the party is over!
        TransactionFinishCallback finish_callback = this.ts.initTransactionFinishCallback(status);
        this.hstore_site.getCoordinator().transactionFinish(this.ts, status, finish_callback);
    }
}
