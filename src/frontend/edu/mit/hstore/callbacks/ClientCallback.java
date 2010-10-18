package edu.mit.hstore.callbacks;

import org.apache.log4j.Logger;

import com.google.protobuf.RpcCallback;

import edu.mit.dtxn.Dtxn;
import edu.mit.hstore.HStoreCoordinatorNode;

/**
 * Callback used to send the final output bytes to the client
 * @author pavlo
 */
public class ClientCallback extends AbstractTxnCallback implements RpcCallback<Dtxn.FinishResponse> {
    private static final Logger LOG = Logger.getLogger(ClientCallback.class);
    
    private final byte output[];
    private final boolean commit;
    
    public ClientCallback(HStoreCoordinatorNode hstore_coordinator, long txn_id, byte output[], boolean commit, RpcCallback<byte[]> done) {
        super(hstore_coordinator, txn_id, done);
        this.output = output;
        this.commit = commit;
    }
    
    /**
     * We can finally send out the final answer to the client
     */
    @Override
    public void run(Dtxn.FinishResponse parameter) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("Sending final response to client for txn #" + this.txn_id + " [" +
                      "status=" + (this.commit ? "COMMIT" : "ABORT") + ", " + 
                      "payload=" + parameter.hasPayload() + ", " +
                      "bytes=" + this.output.length + "]");
        }
        this.hstore_coordinator.completeTransaction(this.txn_id);
        this.done.run(this.output);
    }
} // END CLASS
