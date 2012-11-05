package edu.brown.hstore.callbacks;

import org.apache.log4j.Logger;

import edu.brown.hstore.HStoreConstants;
import edu.brown.hstore.HStoreSite;
import edu.brown.hstore.TransactionQueueManager;
import edu.brown.hstore.Hstoreservice.Status;
import edu.brown.hstore.Hstoreservice.TransactionInitResponse;
import edu.brown.hstore.txns.LocalTransaction;
import edu.brown.logging.LoggerUtil;
import edu.brown.logging.LoggerUtil.LoggerBoolean;
import edu.brown.utils.PartitionSet;

/**
 * This callback is meant to block a transaction from executing until all of the
 * partitions that it needs come back and say they're ready to execute it.
 * This is created at the base partition's site.
 * @author pavlo
 */
public class TransactionInitCallback extends AbstractTransactionCallback<LocalTransaction, TransactionInitResponse, TransactionInitResponse> {
    private static final Logger LOG = Logger.getLogger(TransactionInitCallback.class);
    private static final LoggerBoolean debug = new LoggerBoolean(LOG.isDebugEnabled());
    private static final LoggerBoolean trace = new LoggerBoolean(LOG.isTraceEnabled());
    static {
        LoggerUtil.attachObserver(LOG, debug, trace);
    }
    
    private final PartitionSet partitions = new PartitionSet();
    private transient int reject_partition;
    private transient Long reject_txnId = null;
    
    // ----------------------------------------------------------------------------
    // INTIALIZATION
    // ----------------------------------------------------------------------------
    
    /**
     * Constructor
     * @param hstore_site
     */
    public TransactionInitCallback(HStoreSite hstore_site) {
        super(hstore_site);
    }

    public void init(LocalTransaction ts) {
        this.reject_partition = HStoreConstants.NULL_PARTITION_ID;
        this.reject_txnId = null;
        this.partitions.clear();
        super.init(ts, ts.getPredictTouchedPartitions().size(), null);
    }
    
    @Override
    protected void unblockTransactionCallback() {
        assert(this.isAborted() == false);
        if (hstore_conf.site.txn_profiling && this.ts.profiler != null) this.ts.profiler.stopInitDtxn();
        if (debug.get())
            LOG.debug(this.ts + " is ready to execute. Passing to HStoreSite");
        hstore_site.transactionStart((LocalTransaction)this.ts, this.ts.getBasePartition());
    }
    
    @Override
    protected boolean abortTransactionCallback(Status status) {
        if (hstore_conf.site.txn_profiling && this.ts.profiler != null) this.ts.profiler.stopInitDtxn();
        if (debug.get())
            LOG.debug(this.ts + " - Transaction was aborted with status " + status);
        
        // If the transaction needs to be restarted, then we'll attempt to requeue it.
        switch (status) {
            case ABORT_RESTART: {
                // If we have the transaction that we got busted up with at the remote site
                // then we'll tell the TransactionQueueManager to unblock it when it gets released
                TransactionQueueManager txnQueueManager = this.hstore_site.getTransactionQueueManager();
                synchronized (this) {
                    if (this.reject_txnId != null) {
                        txnQueueManager.blockTransaction(this.ts, this.reject_partition, this.reject_txnId);
                    } else {
                        // We don't care whether our transaction was rejected or not because we know that
                        // we still need to call TransactionFinish, which will delete the final transaction state
                        txnQueueManager.restartTransaction(this.ts, status);
                    }
                } // SYNCH
                break;
            }
            case ABORT_REJECT:
                this.hstore_site.transactionReject(this.ts, status);
                break;
            default:
                throw new RuntimeException(String.format("Unexpected status %s for %s", status, this.ts));
        } // SWITCH
        
        return (true);
    }
    
    @Override
    protected int runImpl(TransactionInitResponse response) {
        if (debug.get()) LOG.debug(String.format("Got %s with status %s for %s " +
            		               "[partitions=%s, rejectPartition=%s, rejectTxn=%s]",
            		               response.getClass().getSimpleName(),
            		               response.getStatus(),
            		               this.ts, 
            		               response.getPartitionsList(),
            		               (response.hasRejectPartition() ? response.getRejectPartition() : "-"),
            		               (response.hasRejectTransactionId() ? response.getRejectTransactionId() : "-")));
        
        assert(this.ts != null) :
            String.format("Missing LocalTransaction handle for txn #%d [status=%s]",
                          response.getTransactionId(), response.getStatus());
        assert(response.getPartitionsCount() > 0) :
            String.format("No partitions returned in %s for %s",
                          response.getClass().getSimpleName(), this.ts);
        // Otherwise, make sure it's legit
        // HACK HACK HACK
        if (this.ts == null || this.ts.isInitialized() == false || this.ts.getTransactionId() != response.getTransactionId()) {
            String.format("Unexpected %s for a different transaction %s != #%d [expected=#%d, partitions=%s]",
                    response.getClass().getSimpleName(),
                    this.ts, response.getTransactionId(),
                    this.getTransactionId(), response.getPartitionsList());
        }
        
        this.partitions.addAll(response.getPartitionsList());
        if (response.getStatus() != Status.OK || this.isAborted()) {
            // If we were told what the highest transaction id was at the remove partition, then 
            // we will store it so that we can update the TransactionQueueManager later on.
            // We are putting it in a synchronization block just to play it safe.
            if (response.hasRejectTransactionId()) {
                assert(response.hasRejectPartition()) :
                    String.format("%s has a reject txn #%d but is missing reject partition [txn=#%d]",
                                  response.getClass().getSimpleName(), response.getTransactionId(), this.ts);
                synchronized (this) {
                    if (this.reject_txnId == null || this.reject_txnId < response.getRejectTransactionId()) {
                        if (debug.get()) LOG.debug(String.format("%s was rejected at partition %d by txn #%d",
                                                   this.ts, response.getRejectPartition(), response.getRejectTransactionId()));
                        this.reject_partition = response.getRejectPartition();
                        this.reject_txnId = response.getRejectTransactionId();
                    }
                } // SYNCH
            }
            // this.decrementCounter(response.getPartitionsCount());
            this.abort(response.getStatus());
            // return (0);
        }
        return (response.getPartitionsCount());
    }
    
    @Override
    public String toString() {
        return super.toString() + " / Partitions="+ this.partitions;
    }
}