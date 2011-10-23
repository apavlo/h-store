package edu.mit.hstore.callbacks;

import org.apache.log4j.Logger;

import edu.brown.hstore.Hstore;
import edu.brown.hstore.Hstore.Status;
import edu.brown.utils.LoggerUtil;
import edu.brown.utils.LoggerUtil.LoggerBoolean;
import edu.mit.hstore.HStoreSite;
import edu.mit.hstore.dtxn.LocalTransaction;

public class TransactionFinishCallback extends BlockingCallback<Hstore.TransactionFinishResponse, Hstore.TransactionFinishResponse> {
    private static final Logger LOG = Logger.getLogger(TransactionFinishCallback.class);
    private final static LoggerBoolean debug = new LoggerBoolean(LOG.isDebugEnabled());
    private final static LoggerBoolean trace = new LoggerBoolean(LOG.isTraceEnabled());
    static {
        LoggerUtil.attachObserver(LOG, debug, trace);
    }
 
    private LocalTransaction ts;
    private Hstore.Status status;
    
    /**
     * This is important so bare with me here...
     * When we abort a txn from the TransactionInitCallback, it may get all
     * the FINISH acknowledgments back from the remote sites before we get the
     * INIT acknowledgments back. So this flag just says that we're not allowed to
     * call HStoreSite.completeTransaction() until we know that everybody that we were
     * waiting to hear responses from has sent them.
     */
    private boolean can_complete;
    
    /**
     * Constructor
     * @param hstore_site
     */
    public TransactionFinishCallback(HStoreSite hstore_site) {
        super(hstore_site, false);
    }

    public void init(LocalTransaction ts, Hstore.Status status) {
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
        if (this.can_complete)
            hstore_site.completeTransaction(txn_id, status);
    }
    
    @Override
    protected void abortCallback(Status status) {
        assert(false);
    }
    
    @Override
    protected int runImpl(Hstore.TransactionFinishResponse response) {
        if (debug.get())
            LOG.debug(String.format("Got %s with for %s %s [partitions=%s, counter=%d]",
                                    response.getClass().getSimpleName(),
                                    this.ts, this.status, response.getPartitionsList(), this.getCounter()));
        assert(this.ts.getTransactionId() == response.getTransactionId()) :
            String.format("Unexpected %s for a different transaction %s != #%d",
                          response.getClass().getSimpleName(), this.ts, response.getTransactionId());
        
        return (response.getPartitionsCount());
    }
    
    public void disableTransactionCleanup() {
        assert(this.can_complete);
        if (debug.get())
            LOG.debug(String.format("Blocking completeTransaction() for %s", this.ts));
        this.can_complete = false;
    }
    
    public void allowTransactionCleanup() {
        assert(this.can_complete == false);
        if (debug.get())
            LOG.debug(String.format("Allowing completeTransaction() for %s", this.ts));
        this.can_complete = true;
        if (this.getCounter() == 0) this.unblockCallback();
    }
}
