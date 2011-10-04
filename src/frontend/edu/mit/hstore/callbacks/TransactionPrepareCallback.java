package edu.mit.hstore.callbacks;

//import org.apache.log4j.Logger;

import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.log4j.Logger;
import org.voltdb.ClientResponseImpl;

import com.google.protobuf.RpcCallback;

import edu.brown.hstore.Hstore;
import edu.brown.hstore.Hstore.TransactionFinishResponse;
import edu.mit.hstore.HStoreSite;
import edu.mit.hstore.dtxn.LocalTransaction;

/**
 * 
 * @author pavlo
 */
public class TransactionPrepareCallback extends BlockingCallback<byte[], Hstore.TransactionPrepareResponse> {
    private static final Logger LOG = Logger.getLogger(TransactionPrepareCallback.class);

    private static final RpcCallback<Hstore.TransactionFinishResponse> commit_callback = new RpcCallback<TransactionFinishResponse>() {
        @Override
        public void run(TransactionFinishResponse parameter) {
            // Ignore!
        }
    };
    
    private HStoreSite hstore_site;
    private LocalTransaction ts;
    private ClientResponseImpl cresponse;
    private AtomicBoolean aborted = new AtomicBoolean(false);
    
    public TransactionPrepareCallback() {
        // Nothing...
    }
    
    public void init(HStoreSite hstore_site, LocalTransaction ts, ClientResponseImpl cresponse) {
        this.hstore_site = hstore_site;
        this.ts = ts;
        this.cresponse = cresponse;
        
        // TODO: Figure out how many HStoreSites this transaction touched
        int num_sites = 0;
        
        super.init(num_sites, ts.getClientCallback());
    }
    
    @Override
    public void finish() {
        super.finish();
        this.aborted.set(false);
    }
    
    @Override
    public void unblockCallback() {
        // Everybody returned ok, so we'll tell them all commit right now
        this.hstore_site.getMessenger().transactionFinish(this.ts, Hstore.Status.OK, commit_callback);
        
        // At this point all of our HStoreSites came back with an OK on the 2PC PREPARE
        // So that means we can send back the result to the client and then 
        // send the 2PC COMMIT message to all of our friends.
        this.hstore_site.sendClientResponse(this.ts, this.cresponse);
    }
    
    @Override
    public void run(Hstore.TransactionPrepareResponse response) {
        final Hstore.Status status = response.getStatus();
        
        // If any TransactionPrepareResponse comes back with anything but an OK,
        // then the we need to abort the transaction immediately
        if (status != Hstore.Status.OK) {
            // If this is the first response that told us to abort, then we'll
            // send the abort message out 
            if (this.aborted.compareAndSet(false, true)) {
                // Let everybody know that the party is over!
                this.hstore_site.getMessenger().transactionFinish(this.ts, status, commit_callback);
                
                // Change the response's status and send back the result to the client
                this.cresponse.setStatus(status);
                this.hstore_site.sendClientResponse(this.ts, this.cresponse);
            }
        }
        // Otherwise we need to update our counter to keep track of how many OKs that we got
        // back. We'll ignore anything that comes in after we've aborted
        else if (this.aborted.get() == false) {
            super.run(response);
        }
    }
} // END CLASS