package edu.mit.hstore.callbacks;

import org.apache.log4j.Logger;

import com.google.protobuf.RpcCallback;

import edu.brown.hstore.Hstore;
import edu.brown.hstore.Hstore.Status;

/**
 * 
 * @author pavlo
 */
public class TransactionWorkCallback extends BlockingCallback<Hstore.TransactionWorkResponse, Hstore.TransactionWorkResponse.PartitionResult> {
    private static final Logger LOG = Logger.getLogger(TransactionRedirectCallback.class);
    
    protected Hstore.TransactionWorkResponse.Builder builder = null;

    /**
     * Default Constructor
     */
    public TransactionWorkCallback() {
        super();
    }
    
    public void init(long txn_id, int num_partitions, RpcCallback<Hstore.TransactionWorkResponse> orig_callback) {
        super.init(num_partitions, orig_callback);
        this.builder = Hstore.TransactionWorkResponse.newBuilder()
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
            LOG.debug(String.format("Sending back %d partition results for txn #%d",
                                    this.builder.getResultsCount(), this.builder.getTransactionId()));
        }
        this.getOrigCallback().run(this.builder.build());
    }
    
    @Override
    protected void abortCallback(Status status) {
        // Nothing...
    }
    
    @Override
    public void run(Hstore.TransactionWorkResponse.PartitionResult parameter) {
        this.builder.addResults(parameter);
        if (parameter.hasError()) this.builder.setStatus(Hstore.Status.ABORT_UNEXPECTED);
        super.run(parameter);
    }
}