package edu.mit.hstore.callbacks;

//import org.apache.log4j.Logger;

import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.log4j.Logger;
import org.voltdb.ClientResponseImpl;
import org.voltdb.client.ClientResponse;

import com.google.protobuf.ByteString;
import com.google.protobuf.RpcCallback;

import edu.brown.hstore.Hstore;
import edu.brown.hstore.Hstore.TransactionFinishResponse;
import edu.mit.dtxn.Dtxn;
import edu.mit.hstore.HStoreSite;
import edu.mit.hstore.dtxn.LocalTransaction;

/**
 * Unpack a FragmentResponse and send the bytes to the client
 * This will be called by ExecutionSite.sendClientResponse
 * @author pavlo
 */
public class MultiPartitionTxnCallback extends BlockingCallback<byte[], Hstore.TransactionPrepareResponse> {
    private static final Logger LOG = Logger.getLogger(MultiPartitionTxnCallback.class);

    private static final RpcCallback<Hstore.TransactionFinishResponse> commit_callback = new RpcCallback<TransactionFinishResponse>() {
        @Override
        public void run(TransactionFinishResponse parameter) {
            // Ignore!
        }
    };
    
    private final HStoreSite hstore_site;
    private LocalTransaction ts;
    private ClientResponseImpl cresponse;
    private AtomicBoolean aborted = new AtomicBoolean(false);
    
    public MultiPartitionTxnCallback(HStoreSite hstore_site) {
        this.hstore_site = hstore_site;
    }
    
    public void init(LocalTransaction ts, ClientResponseImpl cresponse) {
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
        // then the part is over and we need to abort the transaction immediately
        if (status != Hstore.Status.OK) {
            if (this.aborted.compareAndSet(false, true)) {
                this.hstore_site.getMessenger().transactionFinish(this.ts, status, commit_callback);
                
                // Change the response's status and send back the result to the client
                
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