package edu.mit.hstore.callbacks;

import com.google.protobuf.RpcCallback;

import edu.mit.hstore.HStoreSite;

/**
 * Base class used to perform the final operations of when a txn completes
 * @author pavlo
 */
public abstract class AbstractTxnCallback {
    
    protected final HStoreSite hstore_coordinator;
    protected final RpcCallback<byte[]> done;
    protected final long txn_id;
    
 
    public AbstractTxnCallback(HStoreSite hstore_coordinator, long txn_id, RpcCallback<byte[]> done) {
        this.hstore_coordinator = hstore_coordinator;
        this.txn_id = txn_id;
        this.done = done;
        assert(this.hstore_coordinator != null) : "Null HStoreCoordinatorNode for txn #" + this.txn_id;
    }
    
    public long getTransactionId() {
        return txn_id;
    }
} // END CLASS