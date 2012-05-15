package edu.brown.hstore.callbacks;

import org.apache.log4j.Logger;
import org.voltdb.ClientResponseImpl;
import org.voltdb.client.ClientResponse;

import edu.brown.hstore.HStoreSite;
import edu.brown.hstore.Hstoreservice;
import edu.brown.hstore.Hstoreservice.Status;
import edu.brown.hstore.Hstoreservice.TransactionPrepareResponse;
import edu.brown.hstore.dtxn.LocalTransaction;
import edu.brown.logging.LoggerUtil;
import edu.brown.logging.LoggerUtil.LoggerBoolean;

/**
 * 
 * @author pavlo
 */
public class TransactionPrepareCallback extends AbstractTransactionCallback<byte[], TransactionPrepareResponse> {
    private static final Logger LOG = Logger.getLogger(TransactionPrepareCallback.class);
    private final static LoggerBoolean debug = new LoggerBoolean(LOG.isDebugEnabled());
    private final static LoggerBoolean trace = new LoggerBoolean(LOG.isTraceEnabled());
    static {
        LoggerUtil.attachObserver(LOG, debug, trace);
    }
    
    private ClientResponseImpl cresponse;
    
    /**
     * Constructor
     * @param hstore_site
     */
    public TransactionPrepareCallback(HStoreSite hstore_site) {
        super(hstore_site);
    }
    
    public void init(LocalTransaction ts, ClientResponseImpl cresponse) {
        super.init(ts,
                   ts.getPredictTouchedPartitions().size(),
                   ts.getClientCallback());
        this.cresponse = cresponse;
    }
    
    @Override
    public boolean unblockTransactionCallback() {
        assert(this.cresponse.isInitialized()) :
            "Trying to send back ClientResponse for " + ts + " before it was set!";

        // Everybody returned ok, so we'll tell them all commit right now
        this.finishTransaction(Status.OK);
        
        // At this point all of our HStoreSites came back with an OK on the 2PC PREPARE
        // So that means we can send back the result to the client and then 
        // send the 2PC COMMIT message to all of our friends.
        // We want to do this first because the transaction state could get
        // cleaned-up right away when we call HStoreCoordinator.transactionFinish()
        this.hstore_site.sendClientResponse(this.ts, this.cresponse);
        return (false);
    }
    
    @Override
    protected boolean abortTransactionCallback(Status status) {
        // As soon as we get an ABORT from any partition, fire off the final ABORT 
        // to all of the partitions
        this.finishTransaction(status);
        
        // Change the response's status and send back the result to the client
        this.cresponse.setStatus(status);
        this.hstore_site.sendClientResponse(this.ts, this.cresponse);
        
        return (false);
    }
    
    @Override
    protected int runImpl(TransactionPrepareResponse response) {
        if (debug.get())
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
        if (response.getStatus() != Hstoreservice.Status.OK) {
            this.abort(response.getStatus());
        }
        // Otherwise we need to update our counter to keep track of how many OKs that we got
        // back. We'll ignore anything that comes in after we've aborted
        return response.getPartitionsCount();
    }
} // END CLASS