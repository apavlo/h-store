package edu.mit.hstore.callbacks;

import java.nio.ByteBuffer;

import org.apache.log4j.Logger;
import org.voltdb.ClientResponseImpl;

import com.google.protobuf.RpcCallback;

import edu.mit.dtxn.Dtxn;
import edu.mit.hstore.HStoreSite;
import edu.mit.hstore.dtxn.LocalTransactionState;

/**
 * This callback is invoked after the ExecutionSite has completed processing a single-partition txn
 * @author pavlo
 */
public class SinglePartitionTxnCallback extends AbstractTxnCallback implements RpcCallback<Dtxn.FragmentResponse> {
    private static final Logger LOG = Logger.getLogger(SinglePartitionTxnCallback.class);
    private static final boolean d = LOG.isDebugEnabled();
    
    private final LocalTransactionState ts;
    
    public SinglePartitionTxnCallback(HStoreSite hstore_site, LocalTransactionState ts, int dest_partition, RpcCallback<byte[]> done) {
        super(hstore_site, ts.getTransactionId(), done);
        this.ts = ts;
    }
    
    @Override
    public void run(Dtxn.FragmentResponse response) {
        final Dtxn.FragmentResponse.Status status = response.getStatus();
        final byte output[] = response.getOutput().toByteArray(); // ClientResponse
        final boolean commit = (status == Dtxn.FragmentResponse.Status.OK);
        final boolean mispredict = (status == Dtxn.FragmentResponse.Status.ABORT_MISPREDICT); 
        
        if (d) LOG.debug(String.format("Got callback for %s [bytes=%d, commit=%s, status=%s]", this.ts, output.length, commit, status));

        // If the txn was mispredicted, then we will pass the information over to the HStoreSite
        // so that it can re-execute the transaction. We want to do this first so that the txn gets re-executed
        // as soon as possible...
        if (mispredict) {
            if (d) LOG.debug(String.format("Restarting %s because it mispredicted", this.ts));
            this.hstore_site.misprediction(this.ts, this.done);
            
        // If the txn committed/aborted, then we can send the response directly back to the client here
        // Note that we don't even need to call HStoreSite.finishTransaction() since that doesn't do anything
        // that we haven't already done!
        } else {
            // Check whether we should disable throttling
            boolean throttle = this.hstore_site.checkDisableThrottling(this.txn_id, this.ts.getBasePartition());
            int timestamp = this.hstore_site.getNextServerTimestamp();
            
            ByteBuffer buffer = ByteBuffer.wrap(output);
            ClientResponseImpl.setThrottleFlag(buffer, throttle);
            ClientResponseImpl.setServerTimestamp(buffer, timestamp);
            if (d) LOG.debug(String.format("Sending back ClientResponse to %s [throttle=%s, timestamp=%d]", ts, throttle, timestamp));
            this.done.run(output);
        }

        // But make sure we always call HStoreSite.completeTransaction() so that we cleanup whatever internal
        // state that we may have for this txn regardless of how it finished
        this.hstore_site.completeTransaction(this.txn_id, status);
    }
}