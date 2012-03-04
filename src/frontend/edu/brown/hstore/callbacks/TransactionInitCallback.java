package edu.brown.hstore.callbacks;

import org.apache.log4j.Logger;

import edu.brown.hstore.Hstoreservice;
import edu.brown.hstore.Hstoreservice.Status;
import edu.brown.hstore.Hstoreservice.TransactionInitResponse;
import edu.brown.logging.LoggerUtil;
import edu.brown.logging.LoggerUtil.LoggerBoolean;
import edu.brown.hstore.HStoreSite;
import edu.brown.hstore.dtxn.LocalTransaction;

/**
 * This callback is meant to block a transaction from executing until all of the
 * partitions that it needs come back and say they're ready to execute it
 * @author pavlo
 */
public class TransactionInitCallback extends BlockingCallback<TransactionInitResponse, TransactionInitResponse> {
    private static final Logger LOG = Logger.getLogger(TransactionInitCallback.class);
    private final static LoggerBoolean debug = new LoggerBoolean(LOG.isDebugEnabled());
    private final static LoggerBoolean trace = new LoggerBoolean(LOG.isTraceEnabled());
    static {
        LoggerUtil.attachObserver(LOG, debug, trace);
    }
    
    private LocalTransaction ts;
    private Integer reject_partition = null;
    private Long reject_txnId = null;
    private TransactionFinishCallback finish_callback;
    private final boolean txn_profiling;
    
    /**
     * Constructor
     * @param hstore_site
     */
    public TransactionInitCallback(HStoreSite hstore_site) {
        super(hstore_site, true);
        this.txn_profiling = hstore_site.getHStoreConf().site.txn_profiling;
    }

    public void init(LocalTransaction ts) {
        if (debug.get())
            LOG.debug("Starting new " + this.getClass().getSimpleName() + " for " + ts);
        this.ts = ts;
        this.finish_callback = null;
        this.reject_partition = null;
        this.reject_txnId = null;
        super.init(ts.getTransactionId(), ts.getPredictTouchedPartitions().size(), null);
    }
    
    @Override
    protected void finishImpl() {
        this.ts = null;
    }
    
    @Override
    public boolean isInitialized() {
        return (this.ts != null);
    }
    
    @Override
    protected void unblockCallback() {
        if (this.isAborted() == false) {
            if (debug.get())
                LOG.debug(ts + " is ready to execute. Passing to HStoreSite");
            if (this.txn_profiling) ts.profiler.stopInitDtxn();
            hstore_site.transactionStart(ts, ts.getBasePartition());
        } else {
            assert(this.finish_callback != null);
            this.finish_callback.enableTransactionDelete();
        }
    }
    
    public synchronized void setRejectionInfo(int partition, long txn_id) {
        this.reject_partition = partition;
        this.reject_txnId = txn_id;
    }
    
    @Override
    protected void abortCallback(Status status) {
        assert(this.isInitialized()) : "ORIG TXN: " + this.getTransactionId();
        
        // Then re-queue the transaction. We want to make sure that
        // we use a new LocalTransaction handle because this one is going to get freed
        // We want to do this first because the transaction state could get
        // cleaned-up right away when we call HStoreCoordinator.transactionFinish()
        switch (status) {
            case ABORT_RESTART: {
                // If we have the transaction that we got busted up with at the remote site
                // then we'll tell the TransactionQueueManager to unblock it when it gets released
                synchronized (this) {
                    if (this.reject_txnId != null) {
                        this.hstore_site.getTransactionQueueManager().queueBlockedDTXN(this.ts, this.reject_partition, this.reject_txnId);
                    } else {
                        this.hstore_site.transactionRestart(this.ts, status, false);
                    }
                } // SYNCH
                break;
            }
            case ABORT_THROTTLED:
            case ABORT_REJECT:
                this.hstore_site.transactionReject(this.ts, status, false);
                break;
            default:
                assert(false) : String.format("Unexpected status %s for %s", status, this.ts);
        } // SWITCH
        
        // If we abort, then we have to send out an ABORT to
        // all of the partitions that we originally sent INIT requests too
        // Note that we do this *even* if we haven't heard back from the remote
        // HStoreSite that they've acknowledged our transaction
        // We don't care when we get the response for this
        this.finish_callback = this.ts.initTransactionFinishCallback(status);
        this.finish_callback.disableTransactionDelete();
        this.hstore_site.getCoordinator().transactionFinish(this.ts, status, this.finish_callback);
    }
    
    @Override
    protected int runImpl(TransactionInitResponse response) {
        if (debug.get())
            LOG.debug(String.format("Got %s with status %s for %s [partitions=%s, rejectPartition=%s, rejectTxn=%s]",
                                    response.getClass().getSimpleName(),
                                    response.getStatus(),
                                    this.ts, 
                                    response.getPartitionsList(),
                                    (response.hasRejectPartition() ? response.getRejectPartition() : "-"),
                                    (response.hasRejectTransactionId() ? response.getRejectTransactionId() : "-")));
        assert(this.ts != null) :
            String.format("Missing LocalTransaction handle for txn #%d", response.getTransactionId());
        assert(response.getPartitionsCount() > 0) :
            String.format("No partitions returned in %s for %s", response.getClass().getSimpleName(), this.ts);
        
        Long expected = this.getTransactionId();
        Long ts_txn_id = this.ts.getTransactionId();
        
        // If we get a response that matches our original txn but the LocalTransaction handle 
        // has changed, then we need to will just ignore it
        if (expected.longValue() == response.getTransactionId() &&
            expected.equals(ts_txn_id) == false) {
            if (debug.get()) LOG.debug(String.format("Ignoring %s for a different transaction #%d [origTxn=#%d]",
                                                     response.getClass().getSimpleName(), response.getTransactionId(), expected));
            return (0);
        }
        // Otherwise, make sure it's legit
        assert(ts_txn_id.longValue() == response.getTransactionId()) :
            String.format("Unexpected %s for a different transaction %s != #%d [expected=#%d]",
                          response.getClass().getSimpleName(), this.ts, response.getTransactionId(), expected);
        
        if (response.getStatus() != Hstoreservice.Status.OK || this.isAborted()) {
            // If we were told what the highest transaction id was at the remove partition, then 
            // we will store it so that we can update the TransactionQueueManager later on.
            // We are putting it in a synchronization block just to play it safe.
            if (response.hasRejectTransactionId()) {
                assert(response.hasRejectPartition()) :
                    String.format("%s has a reject txn #%d but is missing reject partition [txn=#%d]",
                                  response.getClass().getSimpleName(), response.getTransactionId(), this.ts);
                synchronized (this) {
                    if (this.reject_txnId == null || this.reject_txnId < response.getRejectTransactionId()) {
                        if (debug.get()) LOG.debug(String.format("%s was rejected at partition by txn #%d",
                                                                 this.ts, response.getRejectPartition(), response.getRejectTransactionId()));
                        this.reject_partition = response.getRejectPartition();
                        this.reject_txnId = response.getRejectTransactionId();
                    }
                } // SYNCH
            }
            this.abort(response.getStatus());
            return (0);
        }
//        long txn_id = response.getTransactionId();
//        TransactionQueueManager manager = hstore_site.getTransactionQueueManager();
//        for (int partition : response.getPartitionsList()) {
//            manager.markAsLastTxnId(partition, txn_id);
//        } // FOR
        return (response.getPartitionsCount());
    }
}