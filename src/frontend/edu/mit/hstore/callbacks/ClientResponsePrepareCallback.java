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
public final class ClientResponsePrepareCallback extends AbstractTxnCallback implements RpcCallback<Dtxn.FragmentResponse> {
    private static final Logger LOG = Logger.getLogger(ClientResponsePrepareCallback.class);
    
    private final TransactionEstimator t_estimator;
    private final long dtxn_txn_id;
    private final int dest_partition;
    
    public ClientResponsePrepareCallback(HStoreSite hstore_site, long txn_id, int dest_partition, TransactionEstimator t_estimator, RpcCallback<byte[]> done) {
        super(hstore_site, txn_id, done);
        assert(done != null);
        this.dtxn_txn_id = txn_id;
        this.dest_partition = dest_partition;
        this.t_estimator = t_estimator;
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
    public void run(Dtxn.FragmentResponse response) {
        LOG.trace("ClientResponsePrepareCallback.run()");
        this.prepareFinish(response.getOutput().toByteArray(), response.getStatus());
    }
    
    /**
     * 
     * @param output
     * @param status
     */
    private void prepareFinish(byte[] output, Dtxn.FragmentResponse.Status status) {
        final boolean commit = (status == Dtxn.FragmentResponse.Status.OK);
        final boolean mispredict = (status == Dtxn.FragmentResponse.Status.ABORT_MISPREDICT); 
        final boolean trace = LOG.isTraceEnabled();
        if (trace) LOG.trace(String.format("Got callback for txn #%d [bytes=%d, commit=%s, status=%s]", this.txn_id, output.length, commit, status));

        // According to the where ever the VoltProcedure was running, our transaction is
        // now complete (either aborted or committed). So we need to tell Dtxn.Coordinator
        // to go fuck itself and send the final messages to everyone that was involved
        // We have to pack in our txn id in the payload
        Dtxn.FinishRequest request = Dtxn.FinishRequest.newBuilder()
                                            .setTransactionId(this.dtxn_txn_id)
                                            .setCommit(commit)
                                            .setPayload(HStoreSite.encodeTxnId(this.txn_id))
                                            .build();

        // If the txn was mispredicted, then we will pass the information over to the HStoreSite
        // so that it can re-execute the transaction
        if (mispredict) {
            this.hstore_site.misprediction(this.txn_id, this.done);
        }
            
        // We *always* need to send out the FinishRequest to the Dtxn.Coordinator (yes, even if it's a mispredict)
        // because we want to make sure that Dtxn.Coordinator cleans up the internal state for this busted transaction
        ClientResponseFinalCallback callback = new ClientResponseFinalCallback(this.hstore_site, this.txn_id, output, status, this.done);
        if (trace) LOG.debug("Calling Dtxn.Coordinator.finish() for txn #" + this.txn_id + " [payload=" + request.hasPayload() + "]");
        this.hstore_site.requestFinish(this.txn_id, request, callback);
        
        // Then clean-up any extra information that we may have for the txn
        if (this.t_estimator != null) {
            if (commit) {
                if (trace) LOG.trace("Telling the TransactionEstimator to COMMIT txn #" + this.txn_id);
                this.t_estimator.commit(this.txn_id);
            } else if (mispredict) {
                if (trace) LOG.trace("Telling the TransactionEstimator to IGNORE txn #" + this.txn_id);
                this.t_estimator.ignore(this.txn_id);
            } else {
                if (trace) LOG.trace("Telling the TransactionEstimator to ABORT txn #" + this.txn_id);
                this.t_estimator.abort(this.txn_id);
            }
        }
    }
} // END CLASS