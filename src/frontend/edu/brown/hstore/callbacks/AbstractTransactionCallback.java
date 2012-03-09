package edu.brown.hstore.callbacks;

import org.apache.log4j.Logger;

import com.google.protobuf.RpcCallback;

import edu.brown.hstore.HStoreSite;
import edu.brown.hstore.Hstoreservice.Status;
import edu.brown.hstore.dtxn.AbstractTransaction;
import edu.brown.hstore.dtxn.LocalTransaction;
import edu.brown.logging.LoggerUtil;
import edu.brown.logging.LoggerUtil.LoggerBoolean;

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
    }

    @Override
    public final boolean isInitialized() {
        return (this.ts != null);
    }
    
    @Override
    protected final void unblockCallback() {
        assert(this.ts != null) :
            "Unexpected null transaction handle for txn #" + this.getTransactionId();
        if (this.isAborted()) {
            assert(this.finishStatus != null);
            this.deleteTransaction(this.finishStatus);
        } else {
            this.unblockTransactionCallback();
        }
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
    }
    
    /**
     * 
     */
    protected abstract void unblockTransactionCallback();
    protected abstract boolean abortTransactionCallback(Status status);
    
    protected final void deleteTransaction(Status status) {
        synchronized (this.ts) {
            if (this.ts.isDeletable()) {
                if (this.txn_profiling) ts.profiler.stopPostFinish();
//                if (trace.get()) 
                    LOG.info(String.format("%s - Deleting from %s [status=%s]",
                                                         this.ts, this.getClass().getSimpleName(), status));
                hstore_site.deleteTransaction(this.getTransactionId(), status);
            } else {
                LOG.info(String.format("%s - Not deleting from %s [status=%s]\n%s",
                                       this.ts, this.getClass().getSimpleName(), status, this.ts.debug()));
            }
        } // SYNCH
    }
    
    /**
     * 
     * @param status
     */
    protected final void finishTransaction(Status status) {
        // Let everybody know that the party is over!
        TransactionFinishCallback finish_callback = this.ts.initTransactionFinishCallback(status);
        this.hstore_site.getCoordinator().transactionFinish(this.ts, status, finish_callback);
    }
    
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
    
}
