package edu.mit.hstore.callbacks;

import org.apache.log4j.Logger;

import com.google.protobuf.ByteString;
import com.google.protobuf.RpcCallback;

import edu.brown.markov.TransactionEstimator;
import edu.mit.dtxn.Dtxn;
import edu.mit.hstore.HStoreSite;

/**
 * This callback is invoked after the ExecutionSite has completed processing a single-partition txn
 * @author pavlo
 */
public class SinglePartitionTxnCallback extends AbstractTxnCallback implements RpcCallback<Dtxn.CoordinatorResponse> {
    private static final Logger LOG = Logger.getLogger(SinglePartitionTxnCallback.class);
    
    private final TransactionEstimator t_estimator;
    private final int dest_partition;
    private final ByteString payload;
    
    public SinglePartitionTxnCallback(HStoreSite hstore_site, long txn_id, int dest_partition, TransactionEstimator t_estimator, RpcCallback<byte[]> done) {
        super(hstore_site, txn_id, done);
        this.dest_partition = dest_partition;
        this.t_estimator = t_estimator;
        this.payload = HStoreSite.encodeTxnId(this.txn_id);
        assert(this.t_estimator != null) : "Null TransactionEstimator for txn #" + this.txn_id;
    }

    /**
     * The original partition id that this txn's control code executed on
     * @return
     */
    public int getOriginalPartitionId() {
        return this.dest_partition;
    }
    
    @Override
    public void run(Dtxn.CoordinatorResponse response) {
        final Dtxn.FragmentResponse.Status status = response.getStatus();
        final byte output[] = response.getResponse(0).getOutput().toByteArray();
        this.processResponse(output, status);
    }
    
    /**
     * 
     * @param output
     * @param status
     */
    public void processResponse(byte output[], Dtxn.FragmentResponse.Status status) {
        final boolean t = LOG.isTraceEnabled();
        final boolean commit = (status == Dtxn.FragmentResponse.Status.OK);
        final boolean mispredict = (status == Dtxn.FragmentResponse.Status.ABORT_MISPREDICT); 
        
        if (t) LOG.trace(String.format("Got callback for txn #%d [bytes=%d, commit=%s, status=%s]", this.txn_id, output.length, commit, status));

        // If the txn was mispredicted, then we will pass the information over to the HStoreSite
        // so that it can re-execute the transaction. We want to do this first so that the txn gets re-executed
        // as soon as possible...
        if (mispredict) this.hstore_site.misprediction(this.txn_id, this.done);

        // According to the where ever the VoltProcedure was running, our transaction is
        // now complete (either aborted or committed). So we need to tell Dtxn.Coordinator
        // to go fuck itself and send the final messages to everyone that was involved
        // We have to pack in our txn id in the payload
        Dtxn.FinishRequest request = Dtxn.FinishRequest.newBuilder()
                                            .setTransactionId(this.txn_id)
                                            .setCommit(commit)
                                            .setPayload(this.payload)
                                            .build();
        
        // We *always* need to send out the FinishRequest to the Dtxn.Coordinator (yes, even if it's a mispredict)
        // because we want to make sure that Dtxn.Coordinator cleans up the internal state for this busted transaction
        ClientResponseFinalCallback callback = new ClientResponseFinalCallback(this.hstore_site, this.txn_id, output, status, this.done);
        if (t) LOG.trace(String.format("Calling Dtxn.Coordinator.finish() for txn #%d [commit=%s, payload=%s]",  this.txn_id, commit, request.hasPayload()));
        this.hstore_site.requestFinish(this.txn_id, request, callback);
        
        // Then clean-up any extra information that we may have for the txn
        if (this.t_estimator != null) {
            if (commit) {
                if (t) LOG.trace("Telling the TransactionEstimator to COMMIT txn #" + this.txn_id);
                this.t_estimator.commit(this.txn_id);
            } else if (mispredict) {
                if (t) LOG.trace("Telling the TransactionEstimator to IGNORE txn #" + this.txn_id);
                this.t_estimator.ignore(this.txn_id);
            } else {
                if (t) LOG.trace("Telling the TransactionEstimator to ABORT txn #" + this.txn_id);
                this.t_estimator.abort(this.txn_id);
            }
        }
    }
    
    

}