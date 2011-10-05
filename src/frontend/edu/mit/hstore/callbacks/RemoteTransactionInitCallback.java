package edu.mit.hstore.callbacks;

import org.apache.log4j.Logger;

import com.google.protobuf.RpcCallback;

import edu.brown.hstore.Hstore;

/**
 * This callback is used to wrap around the network-outbound TransactionInitResponse callback on
 * an HStoreSite that is being asked to participate in a distributed transaction from another
 * HStoreSite. Only when we get all the acknowledgments (through the run method) for the local 
 * partitions at this HStoreSite will we invoke the original callback.
 * @author pavlo
 */
public class RemoteTransactionInitCallback extends BlockingCallback<Hstore.TransactionInitResponse, Integer> {
    private static final Logger LOG = Logger.getLogger(RemoteTransactionInitCallback.class);
            
    private Hstore.TransactionInitResponse.Builder builder = null;
    
    public RemoteTransactionInitCallback() {
        super();
    }
    
    public void init(long txn_id, int num_partitions, RpcCallback<Hstore.TransactionInitResponse> orig_callback) {
        super.init(num_partitions, orig_callback);
        this.builder = Hstore.TransactionInitResponse.newBuilder()
                             .setTransactionId(txn_id)
                             .setStatus(Hstore.Status.OK);
    }
    
    @Override
    protected void finishImpl() {
        this.builder = null;
    }
    
    @Override
    public void unblockCallback() {
        if (LOG.isDebugEnabled()) {
            LOG.debug(String.format("Sending back TransactionInitResponse with status %s for txn #%d",
                                    this.builder.getStatus(), this.builder.getTransactionId()));
        }
        this.getOrigCallback().run(this.builder.build());
    }
    
    @Override
    protected void abortCallback(Hstore.Status status) {
        this.builder.setStatus(status);
        this.unblockCallback();
    }
    
    @Override
    protected int runImpl(Integer parameter) {
        return 1;
    }
}
