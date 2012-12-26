package edu.brown.hstore.callbacks;

import org.apache.log4j.Logger;

import com.google.protobuf.RpcCallback;

import edu.brown.hstore.HStoreSite;
import edu.brown.hstore.TransactionQueueManager;
import edu.brown.hstore.Hstoreservice.Status;
import edu.brown.hstore.Hstoreservice.TransactionInitResponse;
import edu.brown.hstore.txns.LocalTransaction;
import edu.brown.logging.LoggerUtil;
import edu.brown.logging.LoggerUtil.LoggerBoolean;
import edu.brown.utils.PartitionSet;

/**
 * 
 * @author pavlo
 */
public class LocalInitQueueCallback extends PartitionCountingCallback<LocalTransaction> implements RpcCallback<TransactionInitResponse> {
    private static final Logger LOG = Logger.getLogger(LocalInitQueueCallback.class);
    private static final LoggerBoolean debug = new LoggerBoolean(LOG.isDebugEnabled());
    private static final LoggerBoolean trace = new LoggerBoolean(LOG.isTraceEnabled());
    static {
        LoggerUtil.attachObserver(LOG, debug, trace);
    }

    private final TransactionQueueManager txnQueueManager;
    
    // ----------------------------------------------------------------------------
    // INTIALIZATION
    // ----------------------------------------------------------------------------
    
    public LocalInitQueueCallback(HStoreSite hstore_site) {
        super(hstore_site);
        this.txnQueueManager = hstore_site.getTransactionQueueManager();
    }
    
    @Override
    public void init(LocalTransaction ts, PartitionSet partitions) {
        super.init(ts, partitions);
    }
    
    // ----------------------------------------------------------------------------
    // CALLBACK METHODS
    // ----------------------------------------------------------------------------

    @Override
    protected void unblockCallback() {
        assert(this.isAborted() == false);
        
        // HACK: If this is a single-partition txn, then we don't
        // need to submit it for execution because the PartitionExecutor
        // will fire it off right away
        if (this.ts.isPredictSinglePartition() == false) {
            if (debug.get()) LOG.debug(this.ts + " is ready to execute. Passing to HStoreSite");
            this.hstore_site.transactionStart((LocalTransaction)this.ts,
                                              this.ts.getBasePartition());
        }
    }

    @Override
    protected void abortCallback(Status status) {
        // If the transaction needs to be restarted, then we'll attempt to requeue it.
        switch (status) {
            case ABORT_SPECULATIVE:
            case ABORT_RESTART:
                // We don't care whether our transaction was rejected or not because we 
                // know that we still need to call TransactionFinish, which will delete
                // the final transaction state
                this.txnQueueManager.restartTransaction(this.ts, status);
                break;
            case ABORT_REJECT:
                this.hstore_site.transactionReject(this.ts, status);
                break;
            default:
                throw new RuntimeException(String.format("Unexpected status %s for %s", status, this.ts));
        } // SWITCH
    }

    @Override
    protected void finishImpl() {
        // Nothing to do?
    }
    
    // ----------------------------------------------------------------------------
    // RPC CALLBACK
    // ----------------------------------------------------------------------------

    @Override
    public void run(TransactionInitResponse response) {
        if (debug.get()) LOG.debug(String.format("%s - Got %s with status %s from partitions %s",
                                   this.ts, response.getClass().getSimpleName(),
                                   response.getStatus(), response.getPartitionsList()));
        for (Integer partition : response.getPartitionsList()) {
            this.run(partition.intValue());
        } // FOR
    }
}
