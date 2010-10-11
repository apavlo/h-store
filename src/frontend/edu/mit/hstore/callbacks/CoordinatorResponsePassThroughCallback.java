package edu.mit.hstore.callbacks;

import com.google.protobuf.RpcCallback;

import edu.brown.markov.TransactionEstimator;
import edu.mit.dtxn.Dtxn;
import edu.mit.hstore.HStoreCoordinatorNode;

/**
 * Unpack a CoordinatorResponse and send the bytes to the client
 * @author pavlo
 */
public final class CoordinatorResponsePassThroughCallback extends AbstractTxnCallback implements RpcCallback<Dtxn.CoordinatorResponse> {
    
    public CoordinatorResponsePassThroughCallback(HStoreCoordinatorNode hstore_coordinator, long txn_id, TransactionEstimator t_estimator, RpcCallback<byte[]> done) {
        super(hstore_coordinator, txn_id, t_estimator, done);
    }

    @Override
    public void run(Dtxn.CoordinatorResponse response) {
        assert response.getResponseCount() == 1;
        // FIXME(evanj): This callback should call AbstractTxnCallback.run() once we get
        // generate txn working. For now we'll just forward the request back to the client
        // this.done.run(response.getResponse(0).getOutput().toByteArray());
        this.prepareFinish(response.getResponse(0).getOutput().toByteArray(), response.getStatus());
    }
} // END CLASS