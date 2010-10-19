package edu.mit.hstore.callbacks;

import org.apache.log4j.Logger;

import ca.evanjones.protorpc.ProtoRpcController;

import com.google.protobuf.RpcCallback;

import edu.brown.markov.TransactionEstimator;
import edu.mit.dtxn.Dtxn;
import edu.mit.hstore.HStoreCoordinatorNode;

/**
 * Unpack a FragmentResponse and send the bytes to the client
 * @author pavlo
 */
public final class ClientResponsePrepareCallback extends AbstractTxnCallback implements RpcCallback<Dtxn.FragmentResponse> {
    private static final Logger LOG = Logger.getLogger(ClientResponsePrepareCallback.class);
    
    private final TransactionEstimator t_estimator;
    private final long dtxn_txn_id; 
    
    public ClientResponsePrepareCallback(HStoreCoordinatorNode hstore_coordinator, long txn_id, long dtxn_txn_id, TransactionEstimator t_estimator, RpcCallback<byte[]> done) {
        super(hstore_coordinator, txn_id, done);
        assert(done != null);
        this.dtxn_txn_id = dtxn_txn_id;
        this.t_estimator = t_estimator;
        assert(this.t_estimator != null) : "Null TransactionEstimator for txn #" + this.txn_id;
    }
    
    @Override
    public void run(Dtxn.FragmentResponse response) {
        LOG.trace("FragmentResponsePassThroughCallback.run()");
        this.prepareFinish(response.getOutput().toByteArray(), response.getStatus());
    }
    
    /**
     * 
     * @param output
     * @param status
     */
    private void prepareFinish(byte[] output, Dtxn.FragmentResponse.Status status) {
        boolean commit = (status == Dtxn.FragmentResponse.Status.OK);
        final boolean trace = LOG.isTraceEnabled();
        if (trace) LOG.trace("Got callback for txn #" + this.txn_id + " [bytes=" + output.length + ", commit=" + commit + ", status=" + status + "]");
        
        // According to the where ever the VoltProcedure was running, our transaction is
        // now complete (either aborted or committed). So we need to tell Dtxn.Coordinator
        // to go fuck itself and send the final messages to everyone that was involved
        // We have to pack in our txn id in the payload
        Dtxn.FinishRequest request = Dtxn.FinishRequest.newBuilder()
                                            .setTransactionId(this.dtxn_txn_id)
                                            .setCommit(commit)
                                            .setPayload(HStoreCoordinatorNode.encodeTxnId(this.txn_id))
                                            .build();
        ClientResponseFinalCallback callback = new ClientResponseFinalCallback(this.hstore_coordinator, this.txn_id, output, commit, this.done);
        if (trace) LOG.debug("Calling Dtxn.Coordinator.finish() for txn #" + this.txn_id + " [payload=" + request.hasPayload() + "]");
        
        this.hstore_coordinator.getDtxnCoordinator().finish(new ProtoRpcController(), request, callback);
        
        // Then clean-up any extra information that we may have for the txn
        if (this.t_estimator != null) {
            if (commit) {
                if (trace) LOG.trace("Telling the TransactionEstimator to COMMIT txn #" + this.txn_id);
                this.t_estimator.commit(this.txn_id);
            } else {
                if (trace) LOG.trace("Telling the TransactionEstimator to ABORT txn #" + this.txn_id);
                this.t_estimator.abort(this.txn_id);
            }
        }
    }
} // END CLASS