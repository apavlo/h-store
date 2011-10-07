package edu.mit.hstore.callbacks;

import java.util.Collection;

import org.apache.log4j.Logger;

import com.google.protobuf.RpcCallback;

import edu.brown.hstore.Hstore;
import edu.brown.utils.LoggerUtil;
import edu.brown.utils.LoggerUtil.LoggerBoolean;

/**
 * This callback is used to wrap around the network-outbound TransactionInitResponse callback on
 * an HStoreSite that is being asked to participate in a distributed transaction from another
 * HStoreSite. Only when we get all the acknowledgments (through the run method) for the local 
 * partitions at this HStoreSite will we invoke the original callback.
 * @author pavlo
 */
public class RemoteTransactionInitCallback extends BlockingCallback<Hstore.TransactionInitResponse, Integer> {
    private static final Logger LOG = Logger.getLogger(RemoteTransactionInitCallback.class);
    private final static LoggerBoolean debug = new LoggerBoolean(LOG.isDebugEnabled());
    private final static LoggerBoolean trace = new LoggerBoolean(LOG.isTraceEnabled());
    static {
        LoggerUtil.attachObserver(LOG, debug, trace);
    }
            
    private Hstore.TransactionInitResponse.Builder builder = null;
    private Collection<Integer> partitions = null;
    
    public RemoteTransactionInitCallback() {
        super();
    }
    
    public void init(long txn_id, Collection<Integer> partitions, RpcCallback<Hstore.TransactionInitResponse> orig_callback) {
        if (debug.get())
            LOG.debug("Starting new RemoteTransactionInitCallback for txn #" + txn_id);
        super.init(partitions.size(), orig_callback);
        this.partitions = partitions;
        this.builder = Hstore.TransactionInitResponse.newBuilder()
                             .setTransactionId(txn_id)
                             .setStatus(Hstore.Status.OK);
    }
    
    public Collection<Integer> getPartitions() {
        return (this.partitions);
    }
    
    @Override
    protected void finishImpl() {
        this.builder = null;
    }
    
    @Override
    public void unblockCallback() {
        if (LOG.isDebugEnabled()) {
            LOG.debug(String.format("Sending back TransactionInitResponse with status %s for txn #%d",
                                    this.builder.getStatus(), this.builder.getTransactionId()));
        }
        this.getOrigCallback().run(this.builder.build());
    }
    
    @Override
    protected void abortCallback(Hstore.Status status) {
        if (debug.get())
            LOG.debug(String.format("Aborting RemoteTransactionInitCallback for txn #%d [status=%s]",
                                    this.builder.getTransactionId(), status));
        this.builder.setStatus(status);
        this.unblockCallback();
    }
    
    @Override
    protected int runImpl(Integer parameter) {
        return 1;
    }
}
