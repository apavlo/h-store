package edu.brown.hstore.callbacks;

import org.apache.log4j.Logger;

import com.google.protobuf.RpcCallback;

import edu.brown.hstore.HStoreSite;
import edu.brown.hstore.Hstoreservice.Status;
import edu.brown.hstore.Hstoreservice.WorkResult;
import edu.brown.hstore.Hstoreservice.TransactionWorkResponse;
import edu.brown.hstore.txns.AbstractTransaction;
import edu.brown.logging.LoggerUtil;
import edu.brown.logging.LoggerUtil.LoggerBoolean;
import edu.brown.utils.PartitionSet;

/**
 * 
 * @author pavlo
 */
public class RemoteWorkCallback extends PartitionCountingCallback<AbstractTransaction> implements RpcCallback<WorkResult> {
    private static final Logger LOG = Logger.getLogger(RemoteWorkCallback.class);
    private static final LoggerBoolean debug = new LoggerBoolean();
    static {
        LoggerUtil.attachObserver(LOG, debug);
    }
    
    private TransactionWorkResponse.Builder builder = null;
    private RpcCallback<TransactionWorkResponse> orig_callback = null;

    // ----------------------------------------------------------------------------
    // INTIALIZATION
    // ----------------------------------------------------------------------------
    
    /**
     * Default Constructor
     */
    public RemoteWorkCallback(HStoreSite hstore_site) {
        super(hstore_site);
    }
    
    public void init(AbstractTransaction ts, PartitionSet partitions, RpcCallback<TransactionWorkResponse> orig_callback) {
        super.init(ts, partitions);
        this.orig_callback = orig_callback;
        this.builder = TransactionWorkResponse.newBuilder()
                                            .setTransactionId(ts.getTransactionId().longValue())
                                            .setStatus(Status.OK);
    }

    // ----------------------------------------------------------------------------
    // CALLBACK METHODS
    // ----------------------------------------------------------------------------
    
    @Override
    public void unblockCallback() {
        if (debug.val)
            LOG.debug(String.format("%s - Invoking %s to send back %d partition results",
                      this.ts, this.orig_callback.getClass().getSimpleName(), this.builder.getResultsCount()));
        assert(this.getOrigCounter() == this.builder.getResultsCount()) :
            String.format("The %s for %s has results from %d partitions but it was suppose to have %d.",
                          this.builder.getClass().getSimpleName(), this.ts, this.builder.getResultsCount(), this.getOrigCounter());
        this.orig_callback.run(this.builder.build());
    }
    
    @Override
    protected void abortCallback(int partition, Status status) {
        assert(false) : String.format("Unexpected %s for %s", status, this.ts);
    }
    
    // ----------------------------------------------------------------------------
    // RPC CALLBACK
    // ----------------------------------------------------------------------------
    
    @Override
    public synchronized void run(WorkResult parameter) {
        this.builder.addResults(parameter);
        if (debug.val)
            LOG.debug(String.format("%s - Added new %s from partition %d",
                      this.ts, parameter.getClass().getSimpleName(), parameter.getPartitionId()));
        if (parameter.hasError()) {
            LOG.info(String.format("%s - Marking %s with an error from partition %d",
                    this.ts, this.builder.getClass().getSimpleName(), parameter.getPartitionId()));    
            System.out.println(this.hstore_site.getTransaction(this.builder.getTransactionId())+" "+this.builder.getTransactionId());
        	if (debug.val)
                LOG.debug(String.format("%s - Marking %s with an error from partition %d",
                          this.ts, this.builder.getClass().getSimpleName(), parameter.getPartitionId()));    
            this.builder.setStatus(Status.ABORT_UNEXPECTED);
        }
        this.run(parameter.getPartitionId());
    }

}
