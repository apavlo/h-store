package edu.brown.hstore.callbacks;

import org.apache.log4j.Logger;

import com.google.protobuf.RpcCallback;

import edu.brown.hstore.HStoreSite;
import edu.brown.hstore.Hstoreservice.Status;
import edu.brown.hstore.Hstoreservice.TransactionPrepareResponse;
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
public class RemotePrepareCallback extends PartitionCountingCallback<RemoteTransaction> {
    private static final Logger LOG = Logger.getLogger(RemotePrepareCallback.class);
    private static final LoggerBoolean debug = new LoggerBoolean();
    private static final LoggerBoolean trace = new LoggerBoolean();
    static {
        LoggerUtil.attachObserver(LOG, debug, trace);
    }

    private final PartitionSet localPartitions = new PartitionSet();
    private TransactionPrepareResponse.Builder builder = null;
    private RpcCallback<TransactionPrepareResponse> origCallback;
    
    // ----------------------------------------------------------------------------
    // INTIALIZATION
    // ----------------------------------------------------------------------------
    
    public RemotePrepareCallback(HStoreSite hstore_site) {
        super(hstore_site);
    }
    
    public void init(RemoteTransaction ts, PartitionSet partitions, RpcCallback<TransactionPrepareResponse> origCallback) {
        this.builder = TransactionPrepareResponse.newBuilder()
                            .setTransactionId(ts.getTransactionId().longValue())
                            .setStatus(Status.OK);
        this.origCallback = origCallback;
        
        // Remove non-local partitions
        this.localPartitions.clear();
        this.localPartitions.addAll(partitions);
        this.localPartitions.retainAll(this.hstore_site.getLocalPartitionIds());
        
        super.init(ts, this.localPartitions);
    }
    
    // ----------------------------------------------------------------------------
    // CALLBACK METHODS
    // ----------------------------------------------------------------------------

    @Override
    protected void unblockCallback() {
        if (debug.val)
            LOG.debug(String.format("%s - Checking whether we can send back %s with status %s",
                      this.ts, TransactionPrepareResponse.class.getSimpleName(),
                      (this.builder != null ? this.builder.getStatus() : "???")));
        if (this.builder != null) {
            if (debug.val)
                LOG.debug(String.format("%s - Sending %s to %s with status %s",
                          this.ts,
                          TransactionPrepareResponse.class.getSimpleName(),
                          this.origCallback.getClass().getSimpleName(),
                          this.builder.getStatus()));
            
            // Ok so where's what going on here. We need to send back
            // an abort message, so we're going use the builder that we've been 
            // working on and send out the bomb back to the base partition tells it that this
            // transaction is kaput at this HStoreSite.
            this.builder.clearPartitions();
            PartitionSet partitions = this.getPartitions();
            for (int partition : partitions.values()) {
                assert(this.hstore_site.isLocalPartition(partition));
                this.builder.addPartitions(partition);
            } // FOR

            assert(this.builder.getPartitionsList() != null) :
                String.format("The %s for %s has no results but it was suppose to have %d.",
                              builder.getClass().getSimpleName(), this.ts, this.getOrigCounter());
            assert(this.getOrigCounter() == this.builder.getPartitionsCount()) :
                String.format("The %s for %s has results from %d partitions but it was suppose to have %d.",
                              builder.getClass().getSimpleName(), this.ts, builder.getPartitionsCount(), this.getOrigCounter());
            assert(this.origCallback != null) :
                String.format("The original callback for %s is null!", this.ts);
            
            this.origCallback.run(this.builder.build());
            this.builder = null;
        }
        else if (debug.val) {
            LOG.warn(String.format("%s - No builder is available? Unable to send back %s",
                      this.ts, TransactionPrepareResponse.class.getSimpleName()));
        }
    }
    
    @Override
    protected void abortCallback(int partition, Status status) {
        // Uh... this might have already been sent out?
        if (this.builder != null) {
            if (debug.val)
                LOG.debug(String.format("%s - Aborting %s with status %s",
                          this.ts, this.getClass().getSimpleName(), status));
            
            // Ok so where's what going on here. We need to send back
            // an abort message, so we're going use the builder that we've been 
            // working on and send out the bomb back to the base partition tells it that this
            // transaction is kaput at this HStoreSite.
            this.builder.setStatus(status);
            this.builder.clearPartitions();
            this.builder.addAllPartitions(this.getPartitions());
            
            assert(this.origCallback != null) :
                String.format("The original callback for %s is null!", this.ts);
            
            this.origCallback.run(this.builder.build());
            this.builder = null;
        }
        else if (debug.val) {
            LOG.warn(String.format("%s - No builder is available? Unable to send back %s",
                     this.ts, TransactionPrepareResponse.class.getSimpleName()));
        }
    }
}
