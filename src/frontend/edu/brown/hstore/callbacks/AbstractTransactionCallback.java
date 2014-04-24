package edu.brown.hstore.callbacks;

import org.apache.log4j.Logger;

import com.google.protobuf.RpcCallback;

import edu.brown.hstore.HStoreSite;
import edu.brown.hstore.Hstoreservice.Status;
import edu.brown.hstore.txns.AbstractTransaction;
import edu.brown.hstore.txns.LocalTransaction;
import edu.brown.logging.LoggerUtil;
import edu.brown.logging.LoggerUtil.LoggerBoolean;

/**
 * Special BlockingCallback wrapper for transactions. This class has utility methods for
 * identifying when it is safe to delete a transaction handle from our local HStoreSite. 
 * @author pavlo
 * @param <X> The type of AbstractTransaction handle that this callback uses
 * @param <T> The message type of the original RpcCallback
 * @param <U> The message type that we will accumulate before invoking the original RpcCallback
 */
@Deprecated
public abstract class AbstractTransactionCallback<X extends AbstractTransaction, T, U> extends BlockingRpcCallback<T, U> {
    private static final Logger LOG = Logger.getLogger(AbstractTransactionCallback.class);
    private static final LoggerBoolean debug = new LoggerBoolean();
    private static final LoggerBoolean trace = new LoggerBoolean();
    static {
        LoggerUtil.attachObserver(LOG, debug, trace);
    }
    
    /**
     * The current transaction handle that this callback is assigned to
     */
    protected X ts;
    
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
    
    protected void init(X ts, int counter_val, RpcCallback<T> orig_callback) {
        assert(ts != null) :
            String.format("Null transaction handle in %s", this.getClass().getSimpleName());
        assert(ts.isInitialized()) :
            String.format("Uninitialized transaction handle in %s", this.getClass().getSimpleName());
        
        this.ts = ts;
        if (debug.val) LOG.debug(this.ts + " - Initializing new " + this.getClass().getSimpleName());
        super.init(this.ts.getTransactionId(), counter_val, orig_callback);
    }

    @Override
    protected void finishImpl() {
        this.ts = null;
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
            String.format("Null transaction handle for txn #%s in %s [lastTxn=%s / counter=%d/%d]",
                          this.getTransactionId(), this.getClass().getSimpleName(),
                          this.getOrigTransactionId(), this.getCounter(), this.getOrigCounter());
        assert(this.ts.isInitialized()) :
            String.format("Uninitialized transaction handle for txn #%s in %s [lastTxn=%s / origCounter=%d/%d]",
                          this.getTransactionId(), this.getClass().getSimpleName(),
                          this.getOrigTransactionId(), this.getCounter(), this.getOrigCounter());
        
        if (this.isAborted() == false) {
            this.unblockTransactionCallback();
        }
        this.unblockFinished = true;
    }
    
    @Override
    protected final void abortCallback(Status status) {
        assert(this.isAborted());
        
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
        if (finish && this.ts.isPredictSinglePartition() == false) {
            if (this.ts instanceof LocalTransaction) {
                this.finishTransaction(status);
            } else {
                // FIXME
            }
        }
        this.abortFinished = true;
        this.hstore_site.queueDeleteTransaction(this.txn_id, status);
    }
    
    @SuppressWarnings("unchecked")
    @Override
    public void cancel() {
        super.cancel();
        if (this.getOrigCallback() != null) {
            ((BlockingRpcCallback<T, U>)this.getOrigCallback()).cancel();
        }
    }
    

    /**
     * Transaction unblocking callback implementation
     * @return
     */
    protected abstract void unblockTransactionCallback();
    
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
        if (this.isCanceled() == false && this.isInitialized()) {
            if (this.getCounter() != 0) return (false);
            return ((this.isUnblocked() && this.unblockFinished) || (this.isAborted() && this.abortFinished));
        }
        return (true);
    }
    
    // ----------------------------------------------------------------------------
    // INTERNAL UTILITY METHODS
    // ----------------------------------------------------------------------------
    
    /**
     * Tell the HStoreCoordinator to invoke the TransactionFinish process
     * @param status
     */
    protected final void finishTransaction(Status status) {
        assert(this.ts != null) :
            "Null transaction handle for txn #" + this.getTransactionId();
        if (debug.val) LOG.debug(String.format("%s - Invoking TransactionFinish protocol from %s [status=%s]",
                                   this.ts, this.getClass().getSimpleName(), status));
        
        // Let everybody know that the party is over!
        if (this.ts instanceof LocalTransaction) {
            LocalTransaction local_ts = (LocalTransaction)this.ts;
            LocalFinishCallback callback = ((LocalTransaction)this.ts).getFinishCallback();
            callback.init(local_ts, status);
            this.hstore_site.getCoordinator().transactionFinish(local_ts, status, callback);
        }
    }
    
    @Override
    public String toString() {
        return String.format("%s / %s / Deletable=%s",
                             super.toString(), this.ts, this.allCallbacksFinished());
    }
}
