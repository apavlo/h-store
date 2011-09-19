package edu.mit.hstore.callbacks;

import java.nio.ByteBuffer;

import org.apache.log4j.Logger;
import org.voltdb.ClientResponseImpl;

import com.google.protobuf.RpcCallback;

import edu.mit.dtxn.Dtxn;
import edu.mit.hstore.HStoreSite;

/**
 * Callback used to send the final output bytes to the client
 * @author pavlo
 */
public class ClientResponseFinalCallback extends AbstractTxnCallback implements RpcCallback<Dtxn.FinishResponse> {
    private static final Logger LOG = Logger.getLogger(ClientResponseFinalCallback.class);
    
    private final int base_partition;
    private final byte output[];
    private final Dtxn.FragmentResponse.Status status;
    
    public ClientResponseFinalCallback(HStoreSite hstore_coordinator, long txn_id, int base_partition, byte output[], Dtxn.FragmentResponse.Status status, RpcCallback<byte[]> done) {
        super(hstore_coordinator, txn_id, done);
        this.base_partition = base_partition;
        this.output = output;
        this.status = status;
    }
    
    /**
     * We can finally send out the final answer to the client
     */
    @Override
    public void run(Dtxn.FinishResponse parameter) {
        final boolean d = LOG.isDebugEnabled();
        // But only send the output to the client if this wasn't a mispredict
        assert(this.status != Dtxn.FragmentResponse.Status.ABORT_MISPREDICT);
        
        // Check whether we should disable throttling
        boolean throttle = this.hstore_site.checkDisableThrottling(this.txn_id, this.base_partition);
        int timestamp = this.hstore_site.getNextRequestCounter();
        
        ByteBuffer buffer = ByteBuffer.wrap(output);
        ClientResponseImpl.setThrottleFlag(buffer, throttle);
        ClientResponseImpl.setServerTimestamp(buffer, timestamp);
        
        if (d) LOG.debug(String.format("Invoking final response callback for txn #%d [status=%s, payload=%s, throttle=%s, timestamp=%d, bytes=%d]",  
                                       this.txn_id, this.status, parameter.hasPayload(), throttle, timestamp, this.output.length));
        this.done.run(this.output);
        
        // Always clean-up the transaction with the HStoreSite
        this.hstore_site.completeTransaction(this.txn_id, status);
    }
} // END CLASS
