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
     * Constructor
     * @param hstore_site
     */
    public TransactionFinishCallback(HStoreSite hstore_site) {
        super(hstore_site);
    }

    public void init(LocalTransaction ts, Hstore.Status status) {
        if (debug.get())
            LOG.debug("Initializing " + this.getClass().getSimpleName() + " for " + ts);
        this.ts = ts;
        this.status = status;
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
        hstore_site.completeTransaction(txn_id, status);
    }
    
    @Override
    protected void abortCallback(Status status) {
        assert(false);
    }
    
    @Override
    protected int runImpl(Hstore.TransactionFinishResponse response) {
        if (debug.get())
            LOG.debug(String.format("Got %s with for %s [partitions=%s]",
                                    response.getClass().getSimpleName(),
                                    this.ts, response.getPartitionsList()));
        
        return (response.getPartitionsCount());
    }
}
