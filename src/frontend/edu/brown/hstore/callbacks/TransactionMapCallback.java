package edu.brown.hstore.callbacks;

import org.apache.log4j.Logger;

import edu.brown.hstore.Hstoreservice;
import edu.brown.hstore.Hstoreservice.Status;
import edu.brown.hstore.Hstoreservice.TransactionMapResponse;
import edu.brown.logging.LoggerUtil;
import edu.brown.logging.LoggerUtil.LoggerBoolean;
import edu.brown.hstore.HStoreSite;
import edu.brown.hstore.dtxn.MapReduceTransaction;

/**
 * This callback waits until all of the TransactionMapResponses have come
 * back from all other partitions in the cluster. The unblockCallback will
 * switch the MapReduceTransaction handle into the REDUCE phase and then requeue
 * it at the local HStoreSite
 * @author pavlo
 */
public class TransactionMapCallback extends BlockingCallback<TransactionMapResponse, TransactionMapResponse> {
    private static final Logger LOG = Logger.getLogger(TransactionMapCallback.class);
    private final static LoggerBoolean debug = new LoggerBoolean(LOG.isDebugEnabled());
    private final static LoggerBoolean trace = new LoggerBoolean(LOG.isTraceEnabled());
    static {
        LoggerUtil.attachObserver(LOG, debug, trace);
    }
    
    private MapReduceTransaction ts;
    private TransactionFinishCallback finish_callback;
    
    /**
     * Constructor
     * @param hstore_site
     */
    public TransactionMapCallback(HStoreSite hstore_site) {
        super(hstore_site, true);
    }

    public void init(MapReduceTransaction ts) {
        assert(this.isInitialized() == false) :
            String.format("Trying to initialize %s twice! [origTs=%s, newTs=%s]",
                          this.getClass().getSimpleName(), this.ts, ts);
        if (debug.get())
            LOG.debug("Starting new " + this.getClass().getSimpleName() + " for " + ts);
        this.ts = ts;
        this.finish_callback = null;
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
    
    /**
     * This gets invoked after all of the partitions have finished
     * executing the map phase for this txn
     */
    @Override
    protected void unblockCallback() {
        if (this.isAborted() == false) {
            if (debug.get())
                LOG.debug(ts + " is ready to execute. Passing to HStoreSite " +
                        "<Switching to the 'reduce' phase>.......");
                                    
            ts.setReducePhase();
            assert(ts.isReducePhase());
            
            if(hstore_site.getHStoreConf().site.mr_reduce_blocking){
                if (debug.get())
                    LOG.debug(ts + ": $$$ normal reduce blocking execution way");
                // calling this hstore_site.transactionStart function will block the executing engine on each partition
                hstore_site.transactionStart(ts, ts.getBasePartition());
            } else {
                // throw reduce job to MapReduceHelperThread to do
                if (debug.get())
                    LOG.debug(ts + ": $$$ non-blocking reduce execution by MapReduceHelperThread");
                hstore_site.getMapReduceHelper().queue(ts);
            }
            
        } else {
            assert(this.finish_callback != null);
            this.finish_callback.allowTransactionCleanup();
        }
    }

    @Override
    protected void abortCallback(Status status) {
        assert(this.isInitialized()) : "ORIG TXN: " + this.getOrigTransactionId();
        
        // If we abort, then we have to send out an ABORT to
        // all of the partitions that we originally sent INIT requests too
        // Note that we do this *even* if we haven't heard back from the remote
        // HStoreSite that they've acknowledged our transaction
        // We don't care when we get the response for this
        this.finish_callback = this.ts.initTransactionFinishCallback(status);
        this.finish_callback.disableTransactionCleanup();
        this.hstore_site.getCoordinator().transactionFinish(this.ts, status, this.finish_callback);
    }
    
    @Override
    protected int runImpl(TransactionMapResponse response) {
        if (debug.get())
            LOG.debug(String.format("Got %s with status %s for %s [partitions=%s]",
                                    response.getClass().getSimpleName(),
                                    response.getStatus(),
                                    this.ts, 
                                    response.getPartitionsList()));
        assert(this.ts != null) :
            String.format("Missing MapReduceTransaction handle for txn #%d", response.getTransactionId());
        assert(response.getPartitionsCount() > 0) :
            String.format("No partitions returned in %s for %s", response.getClass().getSimpleName(), this.ts);
        
        long orig_txn_id = this.getOrigTransactionId();
        long resp_txn_id = response.getTransactionId();
        long ts_txn_id = this.ts.getTransactionId();
        
        // If we get a response that matches our original txn but the LocalTransaction handle 
        // has changed, then we need to will just ignore it
        if (orig_txn_id == resp_txn_id && orig_txn_id != ts_txn_id) {
            if (debug.get()) LOG.debug(String.format("Ignoring %s for a different transaction #%d [origTxn=#%d]",
                                                     response.getClass().getSimpleName(), resp_txn_id, orig_txn_id));
            return (0);
        }
        // Otherwise, make sure it's legit
        assert(ts_txn_id == resp_txn_id) :
            String.format("Unexpected %s for a different transaction %s != #%d [expected=#%d]",
                          response.getClass().getSimpleName(), this.ts, resp_txn_id, ts_txn_id);
        
        if (response.getStatus() != Hstoreservice.Status.OK || this.isAborted()) {
            this.abort(response.getStatus());
            return (0);
        }
        return (response.getPartitionsCount());
    }
}