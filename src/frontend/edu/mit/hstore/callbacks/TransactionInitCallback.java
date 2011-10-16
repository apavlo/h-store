package edu.mit.hstore.callbacks;

import org.apache.log4j.Logger;

import edu.brown.hstore.Hstore;
import edu.brown.hstore.Hstore.Status;
import edu.brown.utils.LoggerUtil;
import edu.brown.utils.LoggerUtil.LoggerBoolean;
import edu.mit.hstore.HStoreSite;
import edu.mit.hstore.dtxn.LocalTransaction;

/**
 * This callback is meant to block a transaction from executing until all of the
 * partitions that it needs come back and say they're ready to execute it
 * @author pavlo
 */
public class TransactionInitCallback extends BlockingCallback<Hstore.TransactionInitResponse, Hstore.TransactionInitResponse> {
    private static final Logger LOG = Logger.getLogger(TransactionInitCallback.class);
    private final static LoggerBoolean debug = new LoggerBoolean(LOG.isDebugEnabled());
    private final static LoggerBoolean trace = new LoggerBoolean(LOG.isTraceEnabled());
    static {
        LoggerUtil.attachObserver(LOG, debug, trace);
    }
    
    private LocalTransaction ts;
    
    /**
     * Constructor
     * @param hstore_site
     */
    public TransactionInitCallback(HStoreSite hstore_site) {
        super(hstore_site);
    }

    public void init(LocalTransaction ts) {
        if (debug.get())
            LOG.debug("Starting new " + this.getClass().getSimpleName() + " for " + ts);
        this.ts = ts;
        super.init(ts.getTransactionId(), ts.getPredictTouchedPartitions().size(), null);
    }
    
    @Override
    protected void finishImpl() {
        this.ts = null;
    }
    
    @Override
    public boolean isInitialized() {
        return (this.ts != null && super.isInitialized());
    }
    
    @Override
    protected void unblockCallback() {
        if (debug.get())
            LOG.debug(ts + " is ready to execute. Passing to HStoreSite");
        hstore_site.transactionStart(ts);
    }
    
    @Override
    protected void abortCallback(Status status) {
        assert(status == Hstore.Status.ABORT_REJECT);
        
        // Then re-queue the transaction. We want to make sure that
        // we use a new LocalTransaction handle because this one is going to get freed
        // We want to do this first because the transaction state could get
        // cleaned-up right away when we call HStoreCoordinator.transactionFinish()
        this.hstore_site.transactionRestart(this.ts, status);
        
        // If we abort, then we have to send out an ABORT_REJECT to
        // all of the partitions that we originally sent INIT requests too
        // Note that we do this *even* if we haven't heard back from the remote
        // HStoreSite that they've acknowledged our tranasction
        // We don't care when we get the response for this
        TransactionFinishCallback finish_callback = this.ts.getTransactionFinishCallback(status);
        this.hstore_site.getCoordinator().transactionFinish(this.ts, status, finish_callback);
    }
    
    @Override
    protected int runImpl(Hstore.TransactionInitResponse response) {
        if (debug.get())
            LOG.debug(String.format("Got %s with %d partitions for %s",
                                    response.getClass().getSimpleName(),
                                    response.getPartitionsCount(),
                                    this.ts));
        assert(this.ts != null) :
            String.format("Missing LocalTransaction handle for txn #%d", response.getTransactionId());
        assert(response.getPartitionsCount() > 0) :
            String.format("No partitions returned in %s for %s", response.getClass().getSimpleName(), this.ts);
        
        if (response.getStatus() != Hstore.Status.OK || this.isAborted()) {
            this.abort(response.getStatus());
            return (0);
        }
        return (response.getPartitionsCount());
    }
}