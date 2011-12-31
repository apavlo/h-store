package edu.mit.hstore.callbacks;

import org.apache.log4j.Logger;

import com.google.protobuf.RpcCallback;

import edu.brown.hstore.Hstore;
import edu.brown.hstore.Hstore.Status;
import edu.brown.logging.LoggerUtil;
import edu.brown.logging.LoggerUtil.LoggerBoolean;
import edu.mit.hstore.HStoreSite;

/**
 * 
 * @author pavlo
 */
public class TransactionWorkCallback extends BlockingCallback<Hstore.TransactionWorkResponse, Hstore.TransactionWorkResponse.PartitionResult> {
    private static final Logger LOG = Logger.getLogger(TransactionWorkCallback.class);
    private static final LoggerBoolean debug = new LoggerBoolean(LOG.isDebugEnabled());
    private static final LoggerBoolean trace = new LoggerBoolean(LOG.isTraceEnabled());
    static {
        LoggerUtil.attachObserver(LOG, debug, trace);
    }
    
    protected Hstore.TransactionWorkResponse.Builder builder = null;

    /**
     * Default Constructor
     */
    public TransactionWorkCallback(HStoreSite hstore_site) {
        super(hstore_site, false);
    }
    
    public void init(long txn_id, int num_partitions, RpcCallback<Hstore.TransactionWorkResponse> orig_callback) {
        super.init(txn_id, num_partitions, orig_callback);
        this.builder = Hstore.TransactionWorkResponse.newBuilder()
                                            .setTransactionId(txn_id)
                                            .setStatus(Hstore.Status.OK);
    }
    
    @Override
    protected void finishImpl() {
        this.builder = null;
    }

    @Override
    public void unblockCallback() {
        if (debug.get()) {
            LOG.debug(String.format("Txn #%d - Sending back %d partition results",
                                    this.getTransactionId(), this.builder.getResultsCount()));
        }
        
        assert(this.getOrigCounter() == builder.getResultsCount()) :
            String.format("The %s for txn #%d has results from %d partitions but it was suppose to have %d.",
                          builder.getClass().getSimpleName(), this.getTransactionId(), builder.getResultsCount(), this.getOrigCounter());
        this.getOrigCallback().run(this.builder.build());
    }
    
    @Override
    protected void abortCallback(Status status) {
        assert(false) : String.format("Unexpected %s for txn #%d", status, this.getTransactionId());
    }
    
    @Override
    protected synchronized int runImpl(Hstore.TransactionWorkResponse.PartitionResult parameter) {
        this.builder.addResults(parameter);
        if (debug.get()) LOG.debug(String.format("Added new %s from partition %d for txn #%d",
                                                 parameter.getClass().getSimpleName(), parameter.getPartitionId(), this.getTransactionId()));
        if (parameter.hasError()) {
            if (debug.get()) LOG.debug(String.format("Marking response for txn #%d with an error from partition %d",
                                                     this.getTransactionId(), parameter.getPartitionId()));    
            this.builder.setStatus(Hstore.Status.ABORT_UNEXPECTED);
        }
        return (1);
    }
}