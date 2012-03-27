package edu.brown.hstore.callbacks;

import org.apache.log4j.Logger;

import com.google.protobuf.RpcCallback;

import edu.brown.catalog.CatalogUtil;
import edu.brown.hstore.HStoreSite;
import edu.brown.hstore.Hstoreservice;
import edu.brown.hstore.Hstoreservice.SendDataResponse;
import edu.brown.hstore.Hstoreservice.Status;
import edu.brown.hstore.dtxn.AbstractTransaction;
import edu.brown.hstore.dtxn.MapReduceTransaction;
import edu.brown.logging.LoggerUtil;
import edu.brown.logging.LoggerUtil.LoggerBoolean;

/**
 * This callback waits until all of the TransactionMapResponses have come
 * back from all other partitions in the cluster. The unblockCallback will
 * switch the MapReduceTransaction handle into the REDUCE phase and then requeue
 * it at the local HStoreSite
 * @author pavlo
 */
public class SendDataCallback extends BlockingCallback<AbstractTransaction, SendDataResponse> {
    private static final Logger LOG = Logger.getLogger(SendDataCallback.class);
    private final static LoggerBoolean debug = new LoggerBoolean(LOG.isDebugEnabled());
    private final static LoggerBoolean trace = new LoggerBoolean(LOG.isTraceEnabled());
    static {
        LoggerUtil.attachObserver(LOG, debug, trace);
    }
    
    private AbstractTransaction ts;
    private final int num_sites;
    
    /**
     * Constructor
     * @param hstore_site
     */
    public SendDataCallback(HStoreSite hstore_site) {
        super(hstore_site, false);
        this.num_sites = CatalogUtil.getAllSites(hstore_site.getSite()).size();
    }

    public void init(AbstractTransaction ts, RpcCallback<AbstractTransaction> orig_callback) {
        assert(this.isInitialized() == false) :
            String.format("Trying to initialize %s twice! [origTs=%s, newTs=%s]",
                          this.getClass().getSimpleName(), this.ts, ts);
        if (debug.get())
            LOG.debug("Starting new " + this.getClass().getSimpleName() + " for " + ts);
        this.ts = ts;
        super.init(ts.getTransactionId(), this.num_sites, orig_callback);
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
        assert(this.isAborted() == false);
        if (debug.get())
            LOG.debug(ts + " is ready to execute. Passing to HStoreSite " +
                    " ...<shuffle phases is over>.......<Send all data to partitions already>");
        
        MapReduceTransaction mr_ts = (MapReduceTransaction)ts;
        assert(mr_ts.isShufflePhase());
        // Set reduce in this point is better
        
        // Tell whoever is waiting for us that we have completed sending data
        this.getOrigCallback().run(this.ts);
    }

    @Override
    protected void abortCallback(Status status) {
        assert(this.isInitialized()) : "ORIG TXN: " + this.getTransactionId();
        assert(false) : "Unexpected: " + this.ts;
    }
    
    @Override
    protected int runImpl(SendDataResponse response) {
        if (debug.get())
            LOG.debug(String.format("Got %s with status %s for %s",
                                    response.getClass().getSimpleName(),
                                    response.getStatus(),
                                    this.ts));
                                    //response.getPartitionsList())
                                    
        assert(this.ts != null) :
            String.format("Missing transaction handle for txn #%d", response.getTransactionId());
        
        
        Long orig_txn_id = this.getTransactionId();
        long resp_txn_id = response.getTransactionId();
        Long ts_txn_id = this.ts.getTransactionId();
        
        // If we get a response that matches our original txn but the LocalTransaction handle 
        // has changed, then we need to will just ignore it
        if (orig_txn_id.longValue() == resp_txn_id && orig_txn_id.equals(ts_txn_id) == false) {
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
        if (debug.get()) LOG.debug("SendDataCallback, I am trying to return 1, actually counter is:" + this.getCounter());
        return 1;
    }
}