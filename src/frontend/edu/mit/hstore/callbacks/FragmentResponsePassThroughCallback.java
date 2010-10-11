package edu.mit.hstore.callbacks;

import org.apache.log4j.Logger;

import com.google.protobuf.RpcCallback;

import edu.brown.markov.TransactionEstimator;
import edu.mit.dtxn.Dtxn;
import edu.mit.hstore.HStoreCoordinatorNode;

/**
 * Unpack a FragmentResponse and send the bytes to the client
 * @author pavlo
 */
public final class FragmentResponsePassThroughCallback extends AbstractTxnCallback implements RpcCallback<Dtxn.FragmentResponse> {
    private static final Logger LOG = Logger.getLogger(FragmentResponsePassThroughCallback.class);
    
    public FragmentResponsePassThroughCallback(HStoreCoordinatorNode hstore_coordinator, long txn_id, TransactionEstimator t_estimator, RpcCallback<byte[]> done) {
        super(hstore_coordinator, txn_id, t_estimator, done);
        assert(done != null);
    }
    
    @Override
    public void run(Dtxn.FragmentResponse response) {
        LOG.trace("FragmentResponsePassThroughCallback.run()");
        this.prepareFinish(response.getOutput().toByteArray(), response.getStatus());
    }
} // END CLASS