package edu.brown.hstore.callbacks;

import org.apache.log4j.Logger;

import com.google.protobuf.RpcCallback;

import edu.brown.hstore.HStoreSite;
import edu.brown.hstore.Hstoreservice.Status;
import edu.brown.hstore.Hstoreservice.TransactionPrepareResponse;
import edu.brown.hstore.txns.AbstractTransaction;
import edu.brown.hstore.txns.RemoteTransaction;
import edu.brown.logging.LoggerUtil;
import edu.brown.logging.LoggerUtil.LoggerBoolean;
import edu.brown.utils.PartitionSet;

/**
 * This is callback is used on the remote side of a TransactionPrepareRequest
 * so that the network-outbound callback is not invoked until all of the partitions
 * at this HStoreSite are finished preparing the transaction. 
 * @author pavlo
 */
public class TransactionPrepareWrapperCallback extends BlockingRpcCallback<TransactionPrepareResponse, Integer> {
    private static final Logger LOG = Logger.getLogger(TransactionPrepareWrapperCallback.class);
    private static final LoggerBoolean debug = new LoggerBoolean(LOG.isDebugEnabled());
    static {
        LoggerUtil.attachObserver(LOG, debug);
    }
    
    private TransactionPrepareResponse.Builder builder = null;
    private AbstractTransaction ts;
    private PartitionSet partitions;
    
    public TransactionPrepareWrapperCallback(HStoreSite hstore_site) {
        super(hstore_site, false);
    }
    
    public void init(AbstractTransaction ts, PartitionSet partitions, RpcCallback<TransactionPrepareResponse> orig_callback) {
        assert(this.isInitialized() == false) :
            String.format("Trying to initialize %s twice! [origTs=%s, newTs=%s]",
                          this.getClass().getSimpleName(), this.ts, ts);
        if (debug.get())
            LOG.debug("Starting new " + this.getClass().getSimpleName() + " for " + ts);
        this.ts = ts;
        this.partitions = partitions;
        this.builder = TransactionPrepareResponse.newBuilder()
                             .setTransactionId(ts.getTransactionId())
                             .setStatus(Status.OK);
        super.init(ts.getTransactionId(), partitions.size(), orig_callback);
    }
    
    @Override
    protected void abortCallback(Status status) {
        if (debug.get())
            LOG.debug(String.format("%s - Aborting %s with status %s",
                      this.ts, this.getClass().getSimpleName(), status));
        this.builder.setStatus(status);
        for (int p : this.hstore_site.getLocalPartitionIds().values()) {
            if (this.builder.getPartitionsList().contains(p) == false) {
                this.builder.addPartitions(p);
            }
        } // FOR
        this.unblockCallback();
    }

    @Override
    protected void finishImpl() {
        this.builder = null;
        this.ts = null;
    }
    
    @Override
    public boolean isInitialized() {
        return (this.ts != null && this.builder != null && super.isInitialized());
    }

    @Override
    protected synchronized int runImpl(Integer partition) {
        if (debug.get()) LOG.debug(String.format("%s - Adding partition %d", this.ts, partition));
        assert(this.partitions.contains(partition));
        if (this.isAborted() == false)
            this.builder.addPartitions(partition.intValue());
        return 1;
    }

    @Override
    protected void unblockCallback() {
        if (debug.get()) {
            LOG.debug(String.format("%s - Sending %s to %s with status %s",
                                    this.ts,
                                    TransactionPrepareResponse.class.getSimpleName(),
                                    this.getOrigCallback().getClass().getSimpleName(),
                                    this.builder.getStatus()));
        }
        assert(this.getOrigCounter() == builder.getPartitionsCount()) :
            String.format("The %s for txn %s has results from %d partitions but it was suppose to have %d.",
                          builder.getClass().getSimpleName(), this.ts, builder.getPartitionsCount(), this.getOrigCounter());
        assert(this.getOrigCallback() != null) :
            String.format("The original callback for %s is null!", this.ts);
        
        
        this.getOrigCallback().run(this.builder.build());
        this.builder = null;
    }
}
