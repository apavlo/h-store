package edu.mit.hstore.callbacks;

import org.apache.log4j.Logger;

import com.google.protobuf.RpcCallback;

import edu.brown.hstore.Hstore;
import edu.brown.hstore.Hstore.Status;
import edu.brown.utils.LoggerUtil;
import edu.brown.utils.LoggerUtil.LoggerBoolean;
import edu.mit.hstore.HStoreSite;
import edu.mit.hstore.dtxn.LocalTransaction;

/**
 * This callback is meant to block a transaction from executing until all of the
 * partitions that it needs come back and say they're ready to execute it
 * @author pavlo
 */
public class TransactionInitCallback extends BlockingCallback<Hstore.TransactionInitResponse, Hstore.TransactionInitResponse> {
    private static final Logger LOG = Logger.getLogger(TransactionInitCallback.class);
    private final static LoggerBoolean debug = new LoggerBoolean(LOG.isDebugEnabled());
    private final static LoggerBoolean trace = new LoggerBoolean(LOG.isTraceEnabled());
    static {
        LoggerUtil.attachObserver(LOG, debug, trace);
    }
    
    private static final RpcCallback<Hstore.TransactionFinishResponse> abort_callback = new RpcCallback<Hstore.TransactionFinishResponse>() {
        @Override
        public void run(Hstore.TransactionFinishResponse parameter) {
            // Ignore!
        }
    };
    
    private LocalTransaction ts;
    
    public TransactionInitCallback(HStoreSite hstore_site) {
        super(hstore_site);
    }

    public void init(LocalTransaction ts) {
        if (debug.get())
            LOG.debug("Starting new " + this.getClass().getSimpleName() + " for " + ts);
        this.ts = ts;
        super.init(ts.getPredictTouchedPartitions().size(), null);
    }
    
    @Override
    protected void finishImpl() {
        this.ts = null;
    }
    
    @Override
    protected void unblockCallback() {
        if (debug.get())
            LOG.debug(ts + " is ready to execute. Passing to HStoreSite");
        hstore_site.transactionStart(ts);
    }
    
    @Override
    protected void abortCallback(Status status) {
        assert(status == Hstore.Status.ABORT_REJECT);
        
        // If we abort, then we have to send out an ABORT_REJECT to
        // all of the partitions that we originally sent INIT requests too
        // Note that we do this *even* if we haven't heard back from the remote
        // HStoreSite that they've acknowledged our tranasction
        // We don't care when we get the response for this
        this.hstore_site.getCoordinator().transactionFinish(this.ts, status, abort_callback);

        // Then re-queue the transaction. We want to make sure that
        // we use a new LocalTransaction handle because this one is going to get freed
        this.hstore_site.transactionMispredict(this.ts, this.ts.getClientCallback());
        this.hstore_site.completeTransaction(this.ts.getTransactionId(), status);
    }
    
    @Override
    protected int runImpl(Hstore.TransactionInitResponse parameter) {
        if (debug.get())
            LOG.debug(String.format("Got %s with %d partitions for %s",
                                    parameter.getClass().getSimpleName(), parameter.getPartitionsCount(), this.ts));
        
        if (parameter.getStatus() != Hstore.Status.OK) {
            this.abort(parameter.getStatus());
        }
        return (parameter.getPartitionsCount());
    }
}