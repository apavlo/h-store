package edu.brown.hstore.callbacks;

import org.apache.log4j.Logger;
import org.voltdb.messaging.FastDeserializer;

import com.google.protobuf.RpcCallback;

import edu.brown.hstore.HStoreSite;
import edu.brown.hstore.Hstoreservice.Status;
import edu.brown.hstore.Hstoreservice.TransactionInitResponse;
import edu.brown.hstore.specexec.PrefetchQueryUtil;
import edu.brown.hstore.txns.RemoteTransaction;
import edu.brown.logging.LoggerUtil;
import edu.brown.logging.LoggerUtil.LoggerBoolean;
import edu.brown.utils.PartitionSet;

/**
 * 
 * @author pavlo
 */
public class RemoteInitQueueCallback extends PartitionCountingCallback<RemoteTransaction> {
    private static final Logger LOG = Logger.getLogger(RemoteInitQueueCallback.class);
    private static final LoggerBoolean debug = new LoggerBoolean();
    private static final LoggerBoolean trace = new LoggerBoolean();
    static {
        LoggerUtil.attachObserver(LOG, debug, trace);
    }

    private final boolean prefetch;
    private ThreadLocal<FastDeserializer> prefetchDeserializers;
    private final PartitionSet localPartitions = new PartitionSet();
    private TransactionInitResponse.Builder builder = null;
    private RpcCallback<TransactionInitResponse> origCallback;
    
    // ----------------------------------------------------------------------------
    // INTIALIZATION
    // ----------------------------------------------------------------------------
    
    public RemoteInitQueueCallback(HStoreSite hstore_site) {
        super(hstore_site);
        this.prefetch = hstore_site.getHStoreConf().site.exec_prefetch_queries;
    }
    
    public void init(RemoteTransaction ts, PartitionSet partitions, RpcCallback<TransactionInitResponse> origCallback) {
        this.builder = TransactionInitResponse.newBuilder()
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
    public void run(int partition) {
        if (trace.val)
            LOG.trace(String.format("%s - Prefetch=%s / HasPrefetchFragments=%s",
                      this.ts, this.prefetch, this.ts.hasPrefetchFragments()));
        if (this.prefetch && this.ts.hasPrefetchFragments()) {
            if (this.prefetchDeserializers == null) {
                if (debug.val)
                    LOG.debug(String.format("%s - Checking for prefetch queries at partition %d",
                              this.ts, partition));
                synchronized (this) {
                    this.prefetchDeserializers = new ThreadLocal<FastDeserializer>() {
                        @Override
                        protected FastDeserializer initialValue() {
                            return (new FastDeserializer(new byte[0]));
                        }
                    };
                } // SYNCH
            }
            FastDeserializer fd = this.prefetchDeserializers.get();
            boolean result = PrefetchQueryUtil.dispatchPrefetchQueries(hstore_site, this.ts, fd, partition);
            if (debug.val)
                LOG.debug(String.format("%s - Result from dispatching prefetch queries at partition %d -> %s",
                          this.ts, partition, result));
        }
        super.run(partition);
    }
    
    @Override
    protected void unblockCallback() {
        if (debug.val)
            LOG.debug(String.format("%s - Checking whether we can send back %s with status %s",
                      this.ts, TransactionInitResponse.class.getSimpleName(),
                      (this.builder != null ? this.builder.getStatus() : "???")));
        if (this.builder != null) {
            if (debug.val)
                LOG.debug(String.format("%s - Sending %s to %s with status %s",
                          this.ts,
                          TransactionInitResponse.class.getSimpleName(),
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
                      this.ts, TransactionInitResponse.class.getSimpleName()));
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
            this.builder.setRejectPartition(partition);
            
            assert(this.origCallback != null) :
                String.format("The original callback for %s is null!", this.ts);
            
            this.origCallback.run(this.builder.build());
            this.builder = null;
        }
        else if (debug.val) {
            LOG.warn(String.format("%s - No builder is available? Unable to send back %s",
                     this.ts, TransactionInitResponse.class.getSimpleName()));
        }
    }
}
