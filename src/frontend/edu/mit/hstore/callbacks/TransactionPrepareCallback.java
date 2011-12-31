package edu.mit.hstore.callbacks;

import org.apache.log4j.Logger;
import org.voltdb.ClientResponseImpl;

import edu.brown.hstore.Hstore;
import edu.brown.hstore.Hstore.Status;
import edu.brown.logging.LoggerUtil;
import edu.brown.logging.LoggerUtil.LoggerBoolean;
import edu.mit.hstore.HStoreSite;
import edu.mit.hstore.dtxn.LocalTransaction;

/**
 * 
 * @author pavlo
 */
public class TransactionPrepareCallback extends BlockingCallback<byte[], Hstore.TransactionPrepareResponse> {
    private static final Logger LOG = Logger.getLogger(TransactionPrepareCallback.class);
    private final static LoggerBoolean debug = new LoggerBoolean(LOG.isDebugEnabled());
    private final static LoggerBoolean trace = new LoggerBoolean(LOG.isTraceEnabled());
    static {
        LoggerUtil.attachObserver(LOG, debug, trace);
    }
    
    private LocalTransaction ts;
    private ClientResponseImpl cresponse;

    /**
     * Constructor
     * @param hstore_site
     */
    public TransactionPrepareCallback(HStoreSite hstore_site) {
        super(hstore_site, false);
    }
    
    public void init(LocalTransaction ts) {
        this.ts = ts;
        super.init(ts.getTransactionId(), ts.getPredictTouchedPartitions().size(), ts.getClientCallback());
    }
    
    public void setClientResponse(ClientResponseImpl cresponse) {
        assert(this.cresponse == null);
        this.cresponse = cresponse;
    }
    
    @Override
    public boolean isInitialized() {
        return (this.ts != null && super.isInitialized());
    }
    
    @Override
    public void finishImpl() {
        this.ts = null;
        this.cresponse = null;
    }
    
    @Override
    public void unblockCallback() {
        assert(this.cresponse != null) : "Trying to send back ClientResponse for " + ts + " before it was set!";
        
        // At this point all of our HStoreSites came back with an OK on the 2PC PREPARE
        // So that means we can send back the result to the client and then 
        // send the 2PC COMMIT message to all of our friends.
        // We want to do this first because the transaction state could get
        // cleaned-up right away when we call HStoreCoordinator.transactionFinish()
        this.hstore_site.sendClientResponse(this.ts, this.cresponse);
        
        // Everybody returned ok, so we'll tell them all commit right now
        TransactionFinishCallback finish_callback = this.ts.initTransactionFinishCallback(Hstore.Status.OK);
        this.hstore_site.getCoordinator().transactionFinish(this.ts, Hstore.Status.OK, finish_callback);
    }
    
    @Override
    protected void abortCallback(Status status) {
        // Let everybody know that the party is over!
        TransactionFinishCallback finish_callback = this.ts.initTransactionFinishCallback(status);
        this.hstore_site.getCoordinator().transactionFinish(this.ts, status, finish_callback);
        
        // Change the response's status and send back the result to the client
        this.cresponse.setStatus(status);
        this.hstore_site.sendClientResponse(this.ts, this.cresponse);
    }
    
    @Override
    protected int runImpl(Hstore.TransactionPrepareResponse response) {
        if (debug.get())
            LOG.debug(String.format("Got %s with %d partitions for %s",
                                    response.getClass().getSimpleName(),
                                    response.getPartitionsCount(),
                                    this.ts));
        assert(this.ts.getTransactionId() == response.getTransactionId()) :
            String.format("Unexpected %s for a different transaction %s != #%d",
                          response.getClass().getSimpleName(), this.ts, response.getTransactionId());
        final Hstore.Status status = response.getStatus();
        
        // If any TransactionPrepareResponse comes back with anything but an OK,
        // then the we need to abort the transaction immediately
        if (status != Hstore.Status.OK) {
            this.abort(status);
        }

        // Otherwise we need to update our counter to keep track of how many OKs that we got
        // back. We'll ignore anything that comes in after we've aborted
        return response.getPartitionsCount();
    }
} // END CLASS