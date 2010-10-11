package edu.mit.hstore.callbacks;

import org.apache.log4j.Logger;

import com.google.protobuf.RpcCallback;

import edu.brown.markov.TransactionEstimator;
import edu.mit.dtxn.Dtxn;
import edu.mit.dtxn.Dtxn.CoordinatorResponse;
import edu.mit.hstore.HStoreCoordinatorNode;

/**
 * Placeholder callback that we need to use when we pass the StoredProcedureInvocation through
 * the Dtxn.Coordinator the very first time
 * @author pavlo
 */
public class InitiateCallback extends AbstractTxnCallback implements RpcCallback<Dtxn.CoordinatorResponse> {
    private static final Logger LOG = Logger.getLogger(InitiateCallback.class);
    
    public InitiateCallback(HStoreCoordinatorNode hstore_coordinator, long txnId, TransactionEstimator tEstimator) {
        super(hstore_coordinator, txnId, tEstimator, null);
    }

    @Override
    public void run(CoordinatorResponse parameter) {
        if (LOG.isTraceEnabled())
            LOG.trace("Got initialization callback for txn #" + parameter.getTransactionId() + ". " +
                      "Nothing else to do for now...");
    }
}