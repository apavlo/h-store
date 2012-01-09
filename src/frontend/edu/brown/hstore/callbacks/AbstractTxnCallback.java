package edu.brown.hstore.callbacks;

import com.google.protobuf.RpcCallback;

import edu.brown.hstore.HStoreSite;

/**
 * Base class used to perform the final operations of when a txn completes
 * @author pavlo
 */
public abstract class AbstractTxnCallback {
    
    protected final HStoreSite hstore_site;
    protected final RpcCallback<byte[]> done;
    protected final long txn_id;
    
 
    public AbstractTxnCallback(HStoreSite hstore_site, long txn_id, RpcCallback<byte[]> done) {
        this.hstore_site = hstore_site;
        this.txn_id = txn_id;
        this.done = done;
        assert(this.hstore_site != null) : "Null HStoreSite for txn #" + this.txn_id;
    }
    
    public long getTransactionId() {
        return txn_id;
    }
    
    /**
     * This is the original callback that is used to send the ClientResponse 
     * back to the client. Obviously this can only be invoked once.
     * @return
     */
    public RpcCallback<byte[]> getOriginalRpcCallback() {
        return done;
    }
    
} // END CLASS