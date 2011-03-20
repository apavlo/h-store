package edu.mit.hstore.callbacks;

import org.apache.log4j.Logger;

import com.google.protobuf.RpcCallback;

import edu.mit.dtxn.Dtxn;
import edu.mit.hstore.HStoreSite;

/**
 * This callback is invoked after the ExecutionSite has completed processing a single-partition txn
 * @author pavlo
 */
public class NoDtxnCallback extends AbstractTxnCallback implements RpcCallback<Dtxn.FragmentResponse> {
    private static final Logger LOG = Logger.getLogger(NoDtxnCallback.class);
    
//    private final int dest_partition;
    
    public NoDtxnCallback(HStoreSite hstore_site, long txn_id, int dest_partition, RpcCallback<byte[]> done) {
        super(hstore_site, txn_id, done);
//        this.dest_partition = dest_partition;
    }
    
    @Override
    public void run(Dtxn.FragmentResponse response) {
        final boolean t = LOG.isTraceEnabled();
        
        final Dtxn.FragmentResponse.Status status = response.getStatus();
        final byte output[] = response.getOutput().toByteArray();
        final boolean commit = (status == Dtxn.FragmentResponse.Status.OK);
        final boolean mispredict = (status == Dtxn.FragmentResponse.Status.ABORT_MISPREDICT); 
        
        if (t) LOG.trace(String.format("Got callback for txn #%d [bytes=%d, commit=%s, status=%s]", this.txn_id, output.length, commit, status));

        // If the txn was mispredicted, then we will pass the information over to the HStoreSite
        // so that it can re-execute the transaction. We want to do this first so that the txn gets re-executed
        // as soon as possible...
        if (mispredict) {
            this.hstore_site.misprediction(this.txn_id, this.done);
        // If the txn committed/aborted, then we can send the response directly back to the client here
        // Note that we don't even need to call HStoreSite.finishTransaction() since that doesn't do anything
        // that we haven't already done!
        } else {
            if (t) LOG.trace("Sending back ClientResponse to txn #" + this.txn_id);
            this.done.run(output);   
        }

        // But make sure we always call HStoreSite.completeTransaction() so that we cleanup whatever internal
        // state that we may have for this txn regardless of how it finished
        this.hstore_site.completeTransaction(this.txn_id, status);
    }
}