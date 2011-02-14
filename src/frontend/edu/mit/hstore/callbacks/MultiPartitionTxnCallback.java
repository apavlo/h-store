package edu.mit.hstore.callbacks;

import org.apache.log4j.Logger;

import com.google.protobuf.RpcCallback;

import edu.brown.markov.TransactionEstimator;
import edu.mit.dtxn.Dtxn;
import edu.mit.hstore.HStoreSite;

/**
 * Unpack a FragmentResponse and send the bytes to the client
 * This will be called by ExecutionSite.sendClientResponse
 * @author pavlo
 */
public class MultiPartitionTxnCallback extends AbstractTxnCallback implements RpcCallback<Dtxn.FragmentResponse> {
    private static final Logger LOG = Logger.getLogger(MultiPartitionTxnCallback.class);
    
    private final SinglePartitionTxnCallback inner;
    
    public MultiPartitionTxnCallback(HStoreSite hstore_site, long txn_id, int dest_partition, TransactionEstimator t_estimator, RpcCallback<byte[]> done) {
        super(hstore_site, txn_id, done);
        this.inner = new SinglePartitionTxnCallback(hstore_site, txn_id, dest_partition, t_estimator, done);
    }
    
    /**
     * The original partition id that this txn's control code executed on
     * @return
     */
    public int getOriginalPartitionId() {
        return this.inner.getOriginalPartitionId();
    }
    
    @Override
    public void run(Dtxn.FragmentResponse response) {
        final byte output[] = response.getOutput().toByteArray();
        final Dtxn.FragmentResponse.Status status = response.getStatus();
        this.inner.processResponse(output, status);
    }
} // END CLASS