package edu.brown.hstore.callbacks;

import org.apache.log4j.Logger;

import edu.brown.hstore.HStoreSite;
import edu.brown.hstore.Hstoreservice.Status;
import edu.brown.hstore.Hstoreservice.TransactionFinishResponse;
import edu.brown.hstore.txns.LocalTransaction;
import edu.brown.logging.LoggerUtil;
import edu.brown.logging.LoggerUtil.LoggerBoolean;

public class TransactionFinishCallback extends AbstractTransactionCallback<TransactionFinishResponse, TransactionFinishResponse> {
    private static final Logger LOG = Logger.getLogger(TransactionFinishCallback.class);
    private final static LoggerBoolean debug = new LoggerBoolean(LOG.isDebugEnabled());
    private final static LoggerBoolean trace = new LoggerBoolean(LOG.isTraceEnabled());
    static {
        LoggerUtil.attachObserver(LOG, debug, trace);
    }
 
    private Status status;
    
    /**
     * Constructor
     * @param hstore_site
     */
    public TransactionFinishCallback(HStoreSite hstore_site) {
        super(hstore_site);
    }

    public void init(LocalTransaction ts, Status status) {
        super.init(ts, ts.getPredictTouchedPartitions().size(), null);
        this.status = status;
    }
    
    @Override
    protected boolean unblockTransactionCallback() {
        // There is nothing that we need to do here.
        this.setFinishStatus(status);
        return (true);
    }
    
    @Override
    protected boolean abortTransactionCallback(Status status) {
        String msg = String.format("Invalid State for %s: Trying to abort a finished transaction [status=%s]",
                                   this.ts, status);
        throw new RuntimeException(msg);
    }
    
    @Override
    protected int runImpl(TransactionFinishResponse response) {
        if (debug.get())
            LOG.debug(String.format("%s - Got %s with %s [partitions=%s, counter=%d]",
                                    this.ts, response.getClass().getSimpleName(),
                                    this.status, response.getPartitionsList(), this.getCounter()));

        assert(this.ts != null) :
            String.format("Missing LocalTransaction handle for txn #%d [status=%s]",
                          response.getTransactionId(), this.status);
        // Any response has to match our current transaction handle
        assert(this.ts.getTransactionId().longValue() == response.getTransactionId()) :
            String.format("Unexpected %s for a different transaction %s != #%d [expected=#%d]",
                          response.getClass().getSimpleName(), this.ts, response.getTransactionId(), this.getTransactionId());
        
        return (response.getPartitionsCount());
    }
}
