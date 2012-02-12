package edu.brown.hstore.callbacks;

import org.apache.log4j.Logger;

import edu.brown.hstore.Hstoreservice.TransactionFinishResponse;
import edu.brown.hstore.Hstoreservice.Status;
import edu.brown.logging.LoggerUtil;
import edu.brown.logging.LoggerUtil.LoggerBoolean;
import edu.brown.hstore.HStoreSite;
import edu.brown.hstore.dtxn.LocalTransaction;

public class TransactionFinishCallback extends BlockingCallback<TransactionFinishResponse, TransactionFinishResponse> {
    private static final Logger LOG = Logger.getLogger(TransactionFinishCallback.class);
    private final static LoggerBoolean debug = new LoggerBoolean(LOG.isDebugEnabled());
    private final static LoggerBoolean trace = new LoggerBoolean(LOG.isTraceEnabled());
    static {
        LoggerUtil.attachObserver(LOG, debug, trace);
    }
 
    private LocalTransaction ts;
    private Status status;
    
    /**
     * This is important so bare with me here...
     * When we abort a txn from the TransactionInitCallback, it may get all
     * the FINISH acknowledgments back from the remote sites before we get the
     * INIT acknowledgments back. So this flag just says that we're not allowed to
     * call HStoreSite.completeTransaction() until we know that everybody that we were
     * waiting to hear responses from has sent them.
     */
    private boolean can_complete;
    
    private final boolean txn_profiling;
    
    /**
     * Constructor
     * @param hstore_site
     */
    public TransactionFinishCallback(HStoreSite hstore_site) {
        super(hstore_site, false);
        this.txn_profiling = hstore_site.getHStoreConf().site.txn_profiling;
    }

    public void init(LocalTransaction ts, Status status) {
        if (debug.get())
            LOG.debug("Initializing " + this.getClass().getSimpleName() + " for " + ts);
        this.ts = ts;
        this.status = status;
        this.can_complete = true;
        super.init(ts.getTransactionId(), ts.getPredictTouchedPartitions().size(), null);
    }
    
    @Override
    protected void finishImpl() {
        this.ts = null;
        this.status = null;
    }
    
    @Override
    public boolean isInitialized() {
        return (this.ts != null && super.isInitialized());
    }
    
    @Override
    protected void unblockCallback() {
        if (this.can_complete) {
            if (this.txn_profiling) ts.profiler.stopPostFinish();
            
            hstore_site.completeTransaction(this.getTransactionId(), status);
        }
    }
    
    @Override
    protected void abortCallback(Status status) {
        assert(false);
    }
    
    @Override
    protected int runImpl(TransactionFinishResponse response) {
        if (debug.get())
            LOG.debug(String.format("%s - Got %s with %s [partitions=%s, counter=%d]",
                                    this.ts, response.getClass().getSimpleName(),
                                    this.status, response.getPartitionsList(), this.getCounter()));
        
        long orig_txn_id = this.getOrigTransactionId();
        long resp_txn_id = response.getTransactionId();
        long ts_txn_id = this.ts.getTransactionId();
        
        // If we get a response that matches our original txn but the LocalTransaction handle 
        // has changed, then we need to will just ignore it
        if (orig_txn_id == resp_txn_id && orig_txn_id != ts_txn_id) {
            if (debug.get()) LOG.debug(String.format("Ignoring %s for a different transaction #%d [expected=#%d]",
                                                     response.getClass().getSimpleName(), resp_txn_id, ts_txn_id));
            return (0);
        }
        // Otherwise, make sure it's legit
        assert(ts_txn_id == resp_txn_id) :
            String.format("Unexpected %s for a different transaction %s != #%d [origTxn=#%d]",
                          response.getClass().getSimpleName(), this.ts, resp_txn_id, orig_txn_id);
        
        return (response.getPartitionsCount());
    }
    
    /**
     * Prevent this callback from invoking HStoreSite.completeTransaction
     * until some future time.
     */
    public void disableTransactionCleanup() {
        assert(this.can_complete);
        if (debug.get())
            LOG.debug(String.format("Blocking completeTransaction() for %s", this.ts));
        this.can_complete = false;
    }
    
    /**
     * Allow this callback to invoke HStoreSite.completeTransaction as soon as
     * all of the partitions send back their acknowledgements
     */
    public void allowTransactionCleanup() {
        assert(this.can_complete == false);
        if (debug.get())
            LOG.debug(String.format("Allowing completeTransaction() for %s", this.ts));
        this.can_complete = true;
        if (this.getCounter() == 0) this.unblockCallback();
    }
}
