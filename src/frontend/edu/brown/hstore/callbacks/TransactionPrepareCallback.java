package edu.brown.hstore.callbacks;

import org.apache.log4j.Logger;
import org.voltdb.ClientResponseImpl;

import edu.brown.hstore.HStoreSite;
import edu.brown.hstore.Hstoreservice.Status;
import edu.brown.hstore.Hstoreservice.TransactionPrepareResponse;
import edu.brown.hstore.txns.LocalTransaction;
import edu.brown.logging.LoggerUtil;
import edu.brown.logging.LoggerUtil.LoggerBoolean;

/**
 * This callback is invoked at the base partition once the txn gets all of
 * the 2PC:PREPARE acknowledgments. This is where we will invoke the HStoreSite
 * to send the ClientResponse back to the client.
 * @author pavlo
 */
public class TransactionPrepareCallback extends AbstractTransactionCallback<LocalTransaction, ClientResponseImpl, TransactionPrepareResponse> {
    private static final Logger LOG = Logger.getLogger(TransactionPrepareCallback.class);
    private static final LoggerBoolean debug = new LoggerBoolean(LOG.isDebugEnabled());
    static {
        LoggerUtil.attachObserver(LOG, debug);
    }
    
    /**
     * Constructor
     * @param hstore_site
     */
    public TransactionPrepareCallback(HStoreSite hstore_site) {
        super(hstore_site);
    }
    
    public void init(LocalTransaction ts) {
        int num_partitions = ts.getPredictTouchedPartitions().size();
        super.init(ts, num_partitions, ts.getClientCallback());
    }
    
    @Override
    public void unblockTransactionCallback() {
        if (debug.val) LOG.debug(String.format("%s - Unblocking callback and sending back ClientResponse", this.ts));
        if (hstore_conf.site.txn_profiling && this.ts.profiler != null) {
            if (debug.val) LOG.debug(ts + " - TransactionProfiler.stopPostPrepare() / " + Status.OK);
            this.ts.profiler.stopPostPrepare();
            this.ts.profiler.startPostFinish();
        }

        // Everybody returned ok, so we'll tell them to all commit right now
        // so that they can start executing other things
        this.finishTransaction(Status.OK);
        
        // At this point all of our HStoreSites came back with an OK on the 2PC PREPARE
        // So that means we can send back the result to the client and then 
        // send the 2PC COMMIT message to all of our friends.
        // We want to do this first because the transaction state could get
        // cleaned-up right away when we call HStoreCoordinator.transactionFinish()
        ClientResponseImpl cresponse = this.ts.getClientResponse();
        assert(cresponse.isInitialized()) :
            "Trying to send back ClientResponse for " + ts + " before it was set!";
        this.hstore_site.responseSend(this.ts, cresponse);
    }
    
    @Override
    protected boolean abortTransactionCallback(Status status) {
        if (debug.val) LOG.debug(String.format("%s - Aborting callback and sending back %s ClientResponse", this.ts, status));
        if (hstore_conf.site.txn_profiling && this.ts.profiler != null) {
            if (debug.val) LOG.debug(ts + " - TransactionProfiler.stopPostPrepare() / " + status);
            this.ts.profiler.stopPostPrepare();
            this.ts.profiler.startPostFinish();
        }
        
        // As soon as we get an ABORT from any partition, fire off the final ABORT 
        // to all of the partitions
        this.finishTransaction(status);
        
        // Change the response's status and send back the result to the client
        ClientResponseImpl cresponse = this.ts.getClientResponse();
        cresponse.setStatus(status);
        this.hstore_site.responseSend(this.ts, cresponse);
        
        return (false);
    }
    
    @Override
    protected int runImpl(TransactionPrepareResponse response) {
        if (debug.val)
            LOG.debug(String.format("Got %s with %d partitions for %s",
                                    response.getClass().getSimpleName(),
                                    response.getPartitionsCount(),
                                    this.ts));
        assert(this.ts != null) :
            String.format("Missing LocalTransaction handle for txn #%d [status=%s]",
                          response.getTransactionId(), response.getStatus());
        assert(this.ts.getTransactionId().longValue() == response.getTransactionId()) :
            String.format("Unexpected %s for a different transaction %s != #%d",
                          response.getClass().getSimpleName(), this.ts, response.getTransactionId());
        
        // If any TransactionPrepareResponse comes back with anything but an OK,
        // then the we need to abort the transaction immediately
        // TODO: Instead of OK, we should have different status types for what the
        //       remote partition did. It should be PREPARE_OK or PREPARE_COMMIT
        //       If it's a PREPARE_COMMIT then we know that we don't need to send
        //       a COMMIT message to it in the next round.
        if (response.getStatus() != Status.OK) {
            this.abort(response.getStatus());
        }
        // Otherwise we need to update our counter to keep track of how many OKs that we got
        // back. We'll ignore anything that comes in after we've aborted
        return response.getPartitionsCount();
    }
} // END CLASS