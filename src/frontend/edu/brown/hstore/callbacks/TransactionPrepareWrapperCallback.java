package edu.brown.hstore.callbacks;

import org.apache.log4j.Logger;

import com.google.protobuf.RpcCallback;

import edu.brown.hstore.HStoreSite;
import edu.brown.hstore.Hstoreservice.Status;
import edu.brown.hstore.Hstoreservice.TransactionPrepareResponse;
import edu.brown.hstore.txns.AbstractTransaction;
import edu.brown.logging.LoggerUtil;
import edu.brown.logging.LoggerUtil.LoggerBoolean;
import edu.brown.utils.PartitionSet;

/**
 * This is callback is used on the remote side of a TransactionPrepareRequest
 * so that the network-outbound callback is not invoked until all of the partitions
 * at this HStoreSite are finished preparing the transaction. 
 * @author pavlo
 */
public class TransactionPrepareWrapperCallback extends AbstractTransactionCallback<AbstractTransaction, TransactionPrepareResponse, Integer> {
    private static final Logger LOG = Logger.getLogger(TransactionPrepareWrapperCallback.class);
    private static final LoggerBoolean debug = new LoggerBoolean(LOG.isDebugEnabled());
    static {
        LoggerUtil.attachObserver(LOG, debug);
    }
    
    private TransactionPrepareResponse.Builder builder = null;
    private AbstractTransaction ts;
    private PartitionSet partitions;
    
    public TransactionPrepareWrapperCallback(HStoreSite hstore_site) {
        super(hstore_site);
    }
    
    public void init(AbstractTransaction ts, PartitionSet partitions, RpcCallback<TransactionPrepareResponse> orig_callback) {
        assert(this.isInitialized() == false) :
            String.format("Trying to initialize %s twice! [origTs=%s, newTs=%s]",
                          this.getClass().getSimpleName(), this.ts, ts);
        if (debug.get())
            LOG.debug("Starting new " + this.getClass().getSimpleName() + " for " + ts);
        this.ts = ts;
        this.partitions = partitions;

        // HACK: Don't wait for non-local partitions
        int expected = 0;
        for (int p : this.partitions.values()) {
            if (this.hstore_site.isLocalPartition(p)) expected++;
        } // FOR
        
        this.builder = TransactionPrepareResponse.newBuilder()
                             .setTransactionId(ts.getTransactionId())
                             .setStatus(Status.OK);
        super.init(ts, expected, orig_callback);
    }
    
    @Override
    protected synchronized void finishImpl() {
        super.finishImpl();
        this.builder = null;
    }
    
    @Override
    protected synchronized void unblockTransactionCallback() {
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
    
    @Override
    protected boolean abortTransactionCallback(Status status) {
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
        return (false);
    }
    
    @Override
    protected synchronized int runImpl(Integer partition) {
        if (debug.get()) LOG.debug(String.format("%s - Adding partition %d", this.ts, partition));
        assert(this.partitions.contains(partition));
        if (this.isAborted() == false && this.builder != null)
            this.builder.addPartitions(partition.intValue());
        return 1;
    }
}
