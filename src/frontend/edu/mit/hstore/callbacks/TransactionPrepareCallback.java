package edu.mit.hstore.callbacks;

import org.apache.log4j.Logger;
import org.voltdb.ClientResponseImpl;

import com.google.protobuf.RpcCallback;

import edu.brown.hstore.Hstore;
import edu.brown.hstore.Hstore.Status;
import edu.brown.hstore.Hstore.TransactionFinishResponse;
import edu.brown.utils.LoggerUtil;
import edu.brown.utils.LoggerUtil.LoggerBoolean;
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

    private static final RpcCallback<Hstore.TransactionFinishResponse> commit_callback = new RpcCallback<TransactionFinishResponse>() {
        @Override
        public void run(TransactionFinishResponse parameter) {
            // Ignore!
        }
    };
    
    private final HStoreSite hstore_site;
    private LocalTransaction ts;
    private ClientResponseImpl cresponse;

    /**
     * Constructor
     * @param hstore_site
     */
    public TransactionPrepareCallback(HStoreSite hstore_site) {
        this.hstore_site = hstore_site;
    }
    
    public void init(LocalTransaction ts) {
        this.ts = ts;
        
        // We need to wait for N-1 partitions to send back their acknowledgments
        // The minus one part is so that we don't wait for the base partition to send
        // and acknowledgment.
        super.init(this.ts.getPredictTouchedPartitions().size() - 1, ts.getClientCallback());
    }
    
    public void setClientResponse(ClientResponseImpl cresponse) {
        assert(this.cresponse == null);
        this.cresponse = cresponse;
    }
    
    @Override
    public boolean isInitialized() {
        return (this.ts != null);
    }
    
    @Override
    public void finishImpl() {
        this.ts = null;
        this.cresponse = null;
    }
    
    @Override
    public void unblockCallback() {
        assert(this.cresponse != null) : "Trying to send back ClientResponse for " + ts + " before it was set!";
        
        // Everybody returned ok, so we'll tell them all commit right now
        this.hstore_site.getMessenger().transactionFinish(this.ts, Hstore.Status.OK, commit_callback);
        
        // At this point all of our HStoreSites came back with an OK on the 2PC PREPARE
        // So that means we can send back the result to the client and then 
        // send the 2PC COMMIT message to all of our friends.
        this.hstore_site.sendClientResponse(this.ts, this.cresponse);
    }
    
    @Override
    protected void abortCallback(Status status) {
        // Let everybody know that the party is over!
        this.hstore_site.getMessenger().transactionFinish(this.ts, status, commit_callback);
        
        // Change the response's status and send back the result to the client
        this.cresponse.setStatus(status);
        this.hstore_site.sendClientResponse(this.ts, this.cresponse);
    }
    
    @Override
    public void run(Hstore.TransactionPrepareResponse response) {
        final Hstore.Status status = response.getStatus();
        
        // If any TransactionPrepareResponse comes back with anything but an OK,
        // then the we need to abort the transaction immediately
        if (status != Hstore.Status.OK) {
            this.abort(status);
        }
        // Otherwise we need to update our counter to keep track of how many OKs that we got
        // back. We'll ignore anything that comes in after we've aborted
        else if (this.isAborted() == false && this.getCounter().addAndGet(-1 * response.getPartitionsCount()) == 0) {
            this.unblockCallback();
        }
    }
} // END CLASS