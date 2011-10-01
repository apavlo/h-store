package edu.mit.hstore.callbacks;

import java.util.concurrent.CountDownLatch;

import org.apache.log4j.Logger;

import com.google.protobuf.RpcCallback;

import edu.brown.hstore.Hstore;
import edu.mit.hstore.HStoreSite;

/**
 * This callback is meant to block a transaction from executing until all of the
 * partitions that it needs come back and say they're ready to execute it
 * Currently we use a CountDownLatch, but this is probably not what we to actually
 * do because that means a thread will have to block on it. So we need a better way of notifying
 * ourselves that we can now execute a transaction.
 * @author pavlo
 */
public class InitiateCallback extends AbstractTxnCallback implements RpcCallback<Hstore.TransactionInitRequest> {
    private static final Logger LOG = Logger.getLogger(InitiateCallback.class);
    private final CountDownLatch latch;
    
    public InitiateCallback(HStoreSite hstore_coordinator, long txnId, CountDownLatch latch) {
        super(hstore_coordinator, txnId, null);
        this.latch = latch;
    }

    @Override
    public void run(Hstore.TransactionInitRequest parameter) {
        if (LOG.isTraceEnabled())
            LOG.trace("Got initialization callback for txn #" + this.txn_id + ". " +
                      "Releasing latch!");
        
        // FIXME
        this.latch.countDown();
    }
}