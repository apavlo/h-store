package edu.brown.hstore.callbacks;

import org.apache.log4j.Logger;

import com.google.protobuf.RpcCallback;

import edu.brown.hstore.HStoreSite;
import edu.brown.hstore.Hstoreservice;
import edu.brown.hstore.Hstoreservice.Status;
import edu.brown.hstore.Hstoreservice.WorkResult;
import edu.brown.hstore.Hstoreservice.TransactionWorkResponse;
import edu.brown.logging.LoggerUtil;
import edu.brown.logging.LoggerUtil.LoggerBoolean;

/**
 * 
 * @author pavlo
 */
public class TransactionWorkCallback extends BlockingRpcCallback<TransactionWorkResponse, WorkResult> {
    private static final Logger LOG = Logger.getLogger(TransactionWorkCallback.class);
    private static final LoggerBoolean debug = new LoggerBoolean(LOG.isDebugEnabled());
    static {
        LoggerUtil.attachObserver(LOG, debug);
    }
    
    protected TransactionWorkResponse.Builder builder = null;

    /**
     * Default Constructor
     */
    public TransactionWorkCallback(HStoreSite hstore_site) {
        super(hstore_site, false);
    }
    
    public void init(Long txn_id, int num_partitions, RpcCallback<TransactionWorkResponse> orig_callback) {
        super.init(txn_id, num_partitions, orig_callback);
        this.builder = TransactionWorkResponse.newBuilder()
                                            .setTransactionId(txn_id.longValue())
                                            .setStatus(Hstoreservice.Status.OK);
    }
    
    @Override
    protected void finishImpl() {
        this.builder = null;
    }

    @Override
    public void unblockCallback() {
        if (debug.val) {
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
    protected synchronized int runImpl(WorkResult parameter) {
        this.builder.addResults(parameter);
        if (debug.val) LOG.debug(String.format("Added new %s from partition %d for txn #%d",
                                   parameter.getClass().getSimpleName(), parameter.getPartitionId(), this.getTransactionId()));
        if (parameter.hasError()) {
            if (debug.val) LOG.debug(String.format("Marking response for txn #%d with an error from partition %d",
                                       this.getTransactionId(), parameter.getPartitionId()));    
            this.builder.setStatus(Hstoreservice.Status.ABORT_UNEXPECTED);
        }
        return (1);
    }
}