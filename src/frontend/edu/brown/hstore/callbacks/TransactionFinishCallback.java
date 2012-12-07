package edu.brown.hstore.callbacks;

import org.apache.log4j.Logger;

import edu.brown.hstore.HStoreSite;
import edu.brown.hstore.Hstoreservice.Status;
import edu.brown.hstore.Hstoreservice.TransactionFinishResponse;
import edu.brown.hstore.txns.LocalTransaction;
import edu.brown.logging.LoggerUtil;
import edu.brown.logging.LoggerUtil.LoggerBoolean;

/**
 * This callback waits until we have heard back from all of the partitions
 * that they have finished processing our transaction. Once we get all of the
 * acknowledgments that we need (including any local partitions), then
 * we will queue this transaction up for deletion. If the <b>needs_requeue</b> flag
 * is set to true, then we will requeue it first before deleting 
 * @author pavlo
 */
public class TransactionFinishCallback extends AbstractTransactionCallback<LocalTransaction, TransactionFinishResponse, TransactionFinishResponse> {
    private static final Logger LOG = Logger.getLogger(TransactionFinishCallback.class);
    private static final LoggerBoolean debug = new LoggerBoolean(LOG.isDebugEnabled());
    private static final LoggerBoolean trace = new LoggerBoolean(LOG.isTraceEnabled());
    static {
        LoggerUtil.attachObserver(LOG, debug, trace);
    }
 
    private Status status;
    private boolean needs_requeue = false;
    
    /**
     * Constructor
     * @param hstore_site
     */
    public TransactionFinishCallback(HStoreSite hstore_site) {
        super(hstore_site);
    }

    public void init(LocalTransaction ts, Status status) {
        this.status = status;
        this.needs_requeue = false;
        super.init(ts, ts.getPredictTouchedPartitions().size(), null);
    }
    
    /**
     * Mark that txn for this callback needs to be requeued before it gets 
     * deleted once it gets responses from all of the partitions
     * participating in it 
     */
    public void markForRequeue() {
        assert(this.needs_requeue == false);
        this.needs_requeue = true;
    }
    
    @Override
    protected void unblockTransactionCallback() {
        if (this.needs_requeue) {
            this.hstore_site.transactionRequeue(this.ts, this.status);
        }
        try {
            this.hstore_site.queueDeleteTransaction(this.txn_id, this.status);
        } catch (Throwable ex) {
            String msg = String.format("Failed to queue %s for deletion from %s",
                                       ts, this.getClass().getSimpleName());
            throw new RuntimeException(msg, ex);
        }
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
