package edu.mit.hstore.callbacks;

//import org.apache.log4j.Logger;

import org.apache.log4j.Logger;

import com.google.protobuf.ByteString;
import com.google.protobuf.RpcCallback;

import edu.mit.dtxn.Dtxn;
import edu.mit.hstore.HStoreSite;
import edu.mit.hstore.dtxn.LocalTransactionState;

/**
 * Unpack a FragmentResponse and send the bytes to the client
 * This will be called by ExecutionSite.sendClientResponse
 * @author pavlo
 */
public class MultiPartitionTxnCallback extends AbstractTxnCallback implements RpcCallback<Dtxn.FragmentResponse> {
    private static final Logger LOG = Logger.getLogger(MultiPartitionTxnCallback.class);
    private static final boolean t = LOG.isTraceEnabled();
    
    private final ByteString payload;
    private final LocalTransactionState ts;
    
    public MultiPartitionTxnCallback(HStoreSite hstore_site, LocalTransactionState ts, int dest_partition, RpcCallback<byte[]> done) {
        super(hstore_site, ts.getTransactionId(), done);
        this.payload = HStoreSite.encodeTxnId(this.txn_id);
        this.ts = ts;
    }
    
    @Override
    public void run(Dtxn.FragmentResponse response) {
        final Dtxn.FragmentResponse.Status status = response.getStatus();
        final byte output[] = response.getOutput().toByteArray(); // ClientResponse
        final boolean commit = (status == Dtxn.FragmentResponse.Status.OK);
        final boolean mispredict = (status == Dtxn.FragmentResponse.Status.ABORT_MISPREDICT); 
        
        if (t) LOG.trace(String.format("Got callback for %s [bytes=%d, commit=%s, status=%s]", this.ts, output.length, commit, status));

        // If the txn was mispredicted, then we will pass the information over to the HStoreSite
        // so that it can re-execute the transaction. We want to do this first so that the txn gets re-executed
        // as soon as possible...
        if (mispredict) this.hstore_site.misprediction(this.ts, this.done);

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
        RpcCallback<Dtxn.FinishResponse> callback = null;
        if (mispredict) {
            callback = new MispredictCleanupCallback(this.hstore_site, this.txn_id, status);
        } else {
            callback = new ClientResponseFinalCallback(this.hstore_site, this.txn_id, output, status, this.done);   
        }
        if (t) LOG.trace(String.format("Calling Dtxn.Coordinator.finish() for %s [commit=%s, payload=%s]", this.ts, commit, request.hasPayload()));
        this.hstore_site.requestFinish(this.ts, request, callback);
    }
} // END CLASS