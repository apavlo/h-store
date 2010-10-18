package edu.mit.hstore.callbacks;

import java.util.concurrent.CountDownLatch;

import org.apache.log4j.Logger;

import com.google.protobuf.RpcCallback;

import edu.mit.dtxn.Dtxn;
import edu.mit.hstore.HStoreCoordinatorNode;

/**
 * Placeholder callback that we need to use when we pass the StoredProcedureInvocation through
 * the Dtxn.Coordinator the very first time
 * @author pavlo
 */
public class InitiateCallback extends AbstractTxnCallback implements RpcCallback<Dtxn.CoordinatorResponse> {
    private static final Logger LOG = Logger.getLogger(InitiateCallback.class);
    private final CountDownLatch latch;
    
    public InitiateCallback(HStoreCoordinatorNode hstore_coordinator, long txnId, CountDownLatch latch) {
        super(hstore_coordinator, txnId, null);
        this.latch = latch;
    }

    @Override
    public void run(Dtxn.CoordinatorResponse parameter) {
        if (LOG.isTraceEnabled())
            LOG.trace("Got initialization callback for txn #" + this.txn_id + ". " +
                      "Releasing latch!");
        this.latch.countDown();
    }
}