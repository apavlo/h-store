package edu.brown.hstore.callbacks;

import org.apache.log4j.Logger;

import edu.brown.hstore.HStoreSite;
import edu.brown.hstore.Hstoreservice.Status;
import edu.brown.hstore.Hstoreservice.TransactionFinishResponse;
import edu.brown.hstore.dtxn.LocalTransaction;
import edu.brown.logging.LoggerUtil;
import edu.brown.logging.LoggerUtil.LoggerBoolean;

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
    private boolean readyToDelete;
    
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
        this.readyToDelete = true;
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
        if (this.readyToDelete) {
            if (this.txn_profiling) ts.profiler.stopPostFinish();
            hstore_site.deleteTransaction(this.getTransactionId(), status);
        }
    }
    
    @Override
    protected void abortCallback(Status status) {
        assert(false);
    }
    
    @Override
    protected int runImpl(TransactionFinishResponse response) {
        if (debug.get())
            LOG.debug(String.format("%s - Got %s with %s [partitions=%s, counter=%d, readyToDelete=%s]",
                                    this.ts, response.getClass().getSimpleName(),
                                    this.status, response.getPartitionsList(), this.getCounter(), this.readyToDelete));

        assert(this.ts != null) :
            String.format("Missing LocalTransaction handle for txn #%d [status=%s]",
                          response.getTransactionId(), this.status);
        
        Long expected = this.getTransactionId();
        Long ts_txn_id = this.ts.getTransactionId();
        
        // If we get a response that matches our original txn but the LocalTransaction handle 
        // has changed, then we need to will just ignore it
        if (expected.longValue() == response.getTransactionId() &&
            expected.equals(ts_txn_id) == false) {
            if (debug.get()) LOG.debug(String.format("Ignoring %s for a different transaction #%d [expected=#%d]",
                                                     response.getClass().getSimpleName(), response.getTransactionId(), ts_txn_id));
            return (0);
        }
        // Otherwise, make sure it's legit
        assert(ts_txn_id.longValue() == response.getTransactionId()) :
            String.format("Unexpected %s for a different transaction %s != #%d [expected=#%d]",
                          response.getClass().getSimpleName(), this.ts, response.getTransactionId(), expected);
        
        return (response.getPartitionsCount());
    }
    
    /**
     * Prevent this callback from invoking HStoreSite.deleteTransaction
     * until some future time.
     */
    public void disableTransactionDelete() {
        assert(this.readyToDelete);
        if (debug.get())
            LOG.debug(String.format("%s - Enabling the transaction from being deleted", this.ts));
        this.readyToDelete = false;
    }
    
    /**
     * Allow this callback to invoke HStoreSite.deleteTransaction as soon as
     * all of the partitions send back their acknowledgements
     */
    public void enableTransactionDelete() {
        assert(this.readyToDelete == false);
        if (debug.get())
            LOG.debug(String.format("%s - Enabling the transaction to be deleted", this.ts));
        this.readyToDelete = true;
        if (this.getCounter() == 0) this.unblockCallback();
    }
}
