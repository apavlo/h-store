package edu.mit.hstore.callbacks;

import org.apache.log4j.Logger;

import com.google.protobuf.RpcCallback;

import edu.brown.hstore.Hstore;
import edu.brown.hstore.Hstore.Status;
import edu.mit.hstore.HStoreSite;
import edu.mit.hstore.dtxn.LocalTransaction;

/**
 * This callback is meant to block a transaction from executing until all of the
 * partitions that it needs come back and say they're ready to execute it
 * Currently we use a CountDownLatch, but this is probably not what we to actually
 * do because that means a thread will have to block on it. So we need a better way of notifying
 * ourselves that we can now execute a transaction.
 * @author pavlo
 */
public class LocalTransactionInitCallback extends BlockingCallback<Hstore.TransactionInitResponse, Hstore.TransactionInitResponse> {
    private static final Logger LOG = Logger.getLogger(LocalTransactionInitCallback.class);
    
    private static final RpcCallback<Hstore.TransactionFinishResponse> abort_callback = new RpcCallback<Hstore.TransactionFinishResponse>() {
        @Override
        public void run(Hstore.TransactionFinishResponse parameter) {
            // Ignore!
        }
    };
    
    private final HStoreSite hstore_site;
    private LocalTransaction ts;
    
    public LocalTransactionInitCallback(HStoreSite hstore_site) {
        this.hstore_site = hstore_site;
    }

    public void init(LocalTransaction ts) {
        this.ts = ts;
        super.init(ts.getPredictTouchedPartitions().size(), null);
    }
    
    @Override
    protected void finishImpl() {
        this.ts = null;
    }
    
    @Override
    protected void unblockCallback() {
        // TODO: Queue this LocalTransaction at the HStoreSite
    }
    
    @Override
    protected void abortCallback(Status status) {
        assert(status == Hstore.Status.ABORT_REJECT);
        
        // If we abort, then we have to send out an ABORT_REJECT to
        // all of the partitions that we originally sent INIT requests too
        // Note that we do this *even* if we haven't heard back from the remote
        // HStoreSite that they've acknowledged our tranasction
        // We don't care when we get the response for this
        this.hstore_site.getMessenger().transactionFinish(this.ts, status, abort_callback);

        // Then re-queue the transaction. We want to make sure that
        // we use a new LocalTransaction handle because this one is going to get freed
        this.hstore_site.transactionMispredict(this.ts, this.ts.getClientCallback());
        this.hstore_site.completeTransaction(this.ts.getTransactionId(), status);
    }
    
    @Override
    protected int runImpl(Hstore.TransactionInitResponse parameter) {
        if (parameter.getStatus() != Hstore.Status.OK) {
            this.abort(parameter.getStatus());
        }
        return (parameter.getPartitionsCount());
    }
}