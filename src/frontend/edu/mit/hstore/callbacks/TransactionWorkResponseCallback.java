package edu.mit.hstore.callbacks;

import java.util.concurrent.atomic.AtomicInteger;

import org.apache.log4j.Logger;

import com.google.protobuf.RpcCallback;

import edu.brown.hstore.Hstore;
import edu.brown.utils.Poolable;

/**
 * 
 * @author pavlo
 */
public class TransactionWorkResponseCallback implements RpcCallback<Hstore.TransactionWorkResponse.PartitionResult>, Poolable {
    private static final Logger LOG = Logger.getLogger(TransactionRedirectCallback.class);
    
    protected RpcCallback<Hstore.TransactionWorkResponse> orig_callback;
    protected AtomicInteger partitions = new AtomicInteger(0);
    protected Hstore.TransactionWorkResponse.Builder builder = null;

    /**
     * Default Constructor
     */
    private TransactionWorkResponseCallback() {
        // Nothing to do...
    }
    
    public void init(long txn_id, int num_partitions, RpcCallback<Hstore.TransactionWorkResponse> orig_callback) {
        this.orig_callback = orig_callback;
        this.partitions.set(num_partitions);
        this.builder = Hstore.TransactionWorkResponse.newBuilder()
                                            .setTransactionId(txn_id)
                                            .setStatus(Hstore.TransactionWorkResponse.Status.OK);
    }

    @Override
    public boolean isInitialized() {
        return (this.orig_callback != null);
    }
    
    @Override
    public void finish() {
        this.orig_callback = null;
    }
    
    @Override
    public void run(Hstore.TransactionWorkResponse.PartitionResult parameter) {
        this.builder.addResults(parameter);
        if (parameter.hasError()) this.builder.setStatus(Hstore.TransactionWorkResponse.Status.ERROR);
        
        // If this is the last PartitionResult that we were waiting for, then we'll send back
        // the TransactionWorkResponse to the remote HStoreSite
        if (this.partitions.decrementAndGet() == 0) {
            if (LOG.isDebugEnabled()) {
                LOG.debug(String.format("Sending back %d partition results for txn #%d",
                                        this.builder.getResultsCount(), this.builder.getTransactionId()));
            }
            this.orig_callback.run(this.builder.build());
        }
    }
    
}
