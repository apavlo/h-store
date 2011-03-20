package edu.mit.hstore.callbacks;

import com.google.protobuf.RpcCallback;

import edu.mit.dtxn.Dtxn;
import edu.mit.hstore.HStoreSite;

/**
 * Callback used to send the final output bytes to the client
 * @author pavlo
 */
public class MispredictCleanupCallback extends AbstractTxnCallback implements RpcCallback<Dtxn.FinishResponse> {
    private final Dtxn.FragmentResponse.Status status;
    
    public MispredictCleanupCallback(HStoreSite hstore_site, long txn_id, Dtxn.FragmentResponse.Status status) {
        super(hstore_site, txn_id, null);
        this.status = status;
    }
    
    /**
     * We can finally send out the final answer to the client
     */
    @Override
    public void run(Dtxn.FinishResponse parameter) {
        // The only thing we need to do is call completeTransaction() to clean things up
        this.hstore_site.completeTransaction(this.txn_id, this.status);
    }
} // END CLASS
