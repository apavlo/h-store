package edu.mit.hstore.callbacks;

import org.apache.log4j.Logger;

import com.google.protobuf.ByteString;
import com.google.protobuf.RpcCallback;

import edu.mit.dtxn.Dtxn;
import edu.mit.hstore.HStoreSite;

/**
 * This callback is invoked after the ExecutionSite has completed processing a single-partition txn
 * @author pavlo
 */
public class NoDtxnCallback extends AbstractTxnCallback implements RpcCallback<Dtxn.FragmentResponse> {
    private static final Logger LOG = Logger.getLogger(NoDtxnCallback.class);
    
    private final int dest_partition;
    private final ByteString payload;
    
    public NoDtxnCallback(HStoreSite hstore_site, long txn_id, int dest_partition, RpcCallback<byte[]> done) {
        super(hstore_site, txn_id, done);
        this.dest_partition = dest_partition;
        this.payload = HStoreSite.encodeTxnId(this.txn_id);
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
        final boolean t = LOG.isTraceEnabled();
        
        final Dtxn.FragmentResponse.Status status = response.getStatus();
        final byte output[] = response.getOutput().toByteArray();
        final boolean commit = (status == Dtxn.FragmentResponse.Status.OK);
        final boolean mispredict = (status == Dtxn.FragmentResponse.Status.ABORT_MISPREDICT); 
        
        if (t) LOG.trace(String.format("Got callback for txn #%d [bytes=%d, commit=%s, status=%s]", this.txn_id, output.length, commit, status));

        assert(mispredict == false);

        // According to the where ever the VoltProcedure was running, our transaction is
        // now complete (either aborted or committed). So we need to tell Dtxn.Coordinator
        // to go fuck itself and send the final messages to everyone that was involved
        // We have to pack in our txn id in the payload
        Dtxn.FinishRequest request = Dtxn.FinishRequest.newBuilder()
                                            .setTransactionId(this.txn_id)
                                            .setCommit(commit)
                                            .setPayload(this.payload)
                                            .build();
        if (t) LOG.trace(String.format("Calling Dtxn.Coordinator.finish() for txn #%d [commit=%s, payload=%s]",  this.txn_id, commit, request.hasPayload()));
        this.hstore_site.finish(null, request, null);
        this.done.run(output);
        this.hstore_site.completeTransaction(this.txn_id);
    }
    
    

}