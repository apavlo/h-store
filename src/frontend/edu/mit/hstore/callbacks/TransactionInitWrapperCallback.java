package edu.mit.hstore.callbacks;

import java.util.Collection;

import org.apache.log4j.Logger;

import com.google.protobuf.RpcCallback;

import edu.brown.hstore.Hstore;
import edu.brown.hstore.Hstore.TransactionInitResponse;
import edu.brown.utils.LoggerUtil;
import edu.brown.utils.LoggerUtil.LoggerBoolean;
import edu.mit.hstore.HStoreSite;

/**
 * This callback is used to wrap around the network-outbound TransactionInitResponse callback on
 * an HStoreSite that is being asked to participate in a distributed transaction from another
 * HStoreSite. Only when we get all the acknowledgments (through the run method) for the local 
 * partitions at this HStoreSite will we invoke the original callback.
 * @author pavlo
 */
public class TransactionInitWrapperCallback extends BlockingCallback<Hstore.TransactionInitResponse, Integer> {
    private static final Logger LOG = Logger.getLogger(TransactionInitWrapperCallback.class);
    private final static LoggerBoolean debug = new LoggerBoolean(LOG.isDebugEnabled());
    private final static LoggerBoolean trace = new LoggerBoolean(LOG.isTraceEnabled());
    static {
        LoggerUtil.attachObserver(LOG, debug, trace);
    }
            
    private Hstore.TransactionInitResponse.Builder builder = null;
    private Collection<Integer> partitions = null;
    
    public TransactionInitWrapperCallback(HStoreSite hstore_site) {
        super(hstore_site, false);
    }
    
    public void init(long txn_id, Collection<Integer> partitions, RpcCallback<Hstore.TransactionInitResponse> orig_callback) {
        if (debug.get())
            LOG.debug(String.format("Starting new %s for txn #%d", this.getClass().getSimpleName(), txn_id));

        // Only include local partitions
        int counter = 0;
        Collection<Integer> localPartitions = hstore_site.getLocalPartitionIds();
        for (Integer p : partitions) {
            if (localPartitions.contains(p)) counter++;
        } // FOR
        assert(counter > 0);
        this.partitions = partitions;
        this.builder = Hstore.TransactionInitResponse.newBuilder()
                             .setTransactionId(txn_id)
                             .setStatus(Hstore.Status.OK);
        super.init(txn_id, counter, orig_callback);
    }
    
    public Collection<Integer> getPartitions() {
        return (this.partitions);
    }
    
    @Override
    protected void finishImpl() {
        this.builder = null;
    }
    
    @Override
    public boolean isInitialized() {
        return (this.builder != null && super.isInitialized());
    }
    
    @Override
    public void unblockCallback() {
        if (debug.get()) {
            LOG.debug(String.format("Txn #%d - Sending %s to %s with status %s",
                                    this.txn_id,
                                    TransactionInitResponse.class.getSimpleName(),
                                    this.getOrigCallback().getClass().getSimpleName(),
                                    this.builder.getStatus()));
        }
        this.getOrigCallback().run(this.builder.build());
    }
    
    @Override
    protected void abortCallback(Hstore.Status status) {
        if (debug.get())
            LOG.debug(String.format("Txn #%d - Aborting %s with status %s",
                                    this.txn_id, this.getClass().getSimpleName(), status));
        this.builder.setStatus(status);
        Collection<Integer> localPartitions = hstore_site.getLocalPartitionIds();
        for (Integer p : this.partitions) {
            if (localPartitions.contains(p) && this.builder.getPartitionsList().contains(p) == false) {
                this.builder.addPartitions(p.intValue());
            }
        } // FOR
        this.unblockCallback();
    }
    
    @Override
    protected int runImpl(Integer parameter) {
        if (this.isAborted() == false)
            this.builder.addPartitions(parameter.intValue());
        return 1;
    }
}
