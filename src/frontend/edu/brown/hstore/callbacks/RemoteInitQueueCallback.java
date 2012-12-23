package edu.brown.hstore.callbacks;

import java.io.IOException;
import java.util.List;

import org.apache.log4j.Logger;
import org.voltdb.ParameterSet;
import org.voltdb.exceptions.ServerFaultException;
import org.voltdb.messaging.FastDeserializer;

import com.google.protobuf.ByteString;
import com.google.protobuf.RpcCallback;

import edu.brown.hstore.HStoreSite;
import edu.brown.hstore.Hstoreservice.Status;
import edu.brown.hstore.Hstoreservice.TransactionInitResponse;
import edu.brown.hstore.Hstoreservice.WorkFragment;
import edu.brown.hstore.txns.RemoteTransaction;
import edu.brown.logging.LoggerUtil;
import edu.brown.logging.LoggerUtil.LoggerBoolean;
import edu.brown.profilers.PartitionExecutorProfiler;
import edu.brown.utils.PartitionSet;

/**
 * 
 * @author pavlo
 */
public class RemoteInitQueueCallback extends PartitionCountingCallback<RemoteTransaction> {
    private static final Logger LOG = Logger.getLogger(RemoteInitQueueCallback.class);
    private static final LoggerBoolean debug = new LoggerBoolean(LOG.isDebugEnabled());
    private static final LoggerBoolean trace = new LoggerBoolean(LOG.isTraceEnabled());
    static {
        LoggerUtil.attachObserver(LOG, debug, trace);
    }

    private final boolean prefetch;
    private final FastDeserializer fd = new FastDeserializer(new byte[0]);
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
    
    @Override
    protected void finishImpl() {
        this.origCallback = null;
        this.builder = null;
    }

    @Override
    protected void unblockCallback() {
        if (debug.get()) LOG.debug(String.format("%s - Checking whether we can send back %s with status %s",
                                   this.ts, TransactionInitResponse.class.getSimpleName(),
                                   (this.builder != null ? this.builder.getStatus() : "???")));
        if (this.builder != null) {
            if (debug.get()) {
                LOG.debug(String.format("%s - Sending %s to %s with status %s",
                                        this.ts,
                                        TransactionInitResponse.class.getSimpleName(),
                                        this.origCallback.getClass().getSimpleName(),
                                        this.builder.getStatus()));
            }
            
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
            this.clearCounter();
            
            // start profile idle_waiting_dtxn_time on remote paritions
            if (this.hstore_conf.site.exec_profiling) {
                for (int p : this.hstore_site.getLocalPartitionIds().values()) {
                    if (partitions.contains(p)) {
                        PartitionExecutorProfiler pep = this.hstore_site.getPartitionExecutor(p).getProfiler();
                        assert (pep != null);
                        if (pep.idle_waiting_dtxn_time.isStarted()) pep.idle_waiting_dtxn_time.stop();
                        pep.idle_waiting_dtxn_time.start();
                    }
                } // FOR
            }
            
            // Bundle the prefetch queries in the txn so we can queue them up
            // At this point all of the partitions at this HStoreSite are allocated
            // for executing this txn. We can now check whether it has any embedded
            // queries that need to be queued up for pre-fetching. If so, blast them
            // off to the HStoreSite so that they can be executed in the PartitionExecutor
            // Use txn_id to get the AbstractTransaction handle from the HStoreSite
            if (this.prefetch && ts.hasPrefetchQueries()) {
                // We need to convert our raw ByteString ParameterSets into the actual objects
                List<ByteString> rawParams = ts.getPrefetchRawParameterSets(); 
                int num_parameters = rawParams.size();
                ParameterSet params[] = new ParameterSet[num_parameters]; 
                for (int i = 0; i < params.length; i++) {
                    this.fd.setBuffer(rawParams.get(i).asReadOnlyByteBuffer());
                    try {
                        params[i] = this.fd.readObject(ParameterSet.class);
                    } catch (IOException ex) {
                        String msg = "Failed to deserialize pre-fetch ParameterSet at offset #" + i;
                        throw new ServerFaultException(msg, ex, this.ts.getTransactionId());
                    }
                } // FOR
                ts.attachPrefetchParameters(params);
                
                // Go through all the prefetch WorkFragments and send them off to 
                // the right PartitionExecutor at this HStoreSite.
                for (WorkFragment frag : ts.getPrefetchFragments()) {
                    // XXX: We want to skip any WorkFragments for this txn's base partition.
                    if (frag.getPartitionId() != ts.getBasePartition())
                        hstore_site.transactionWork(ts, frag);
                } // FOR
            }
        }
        else if (debug.get()) {
            LOG.warn(String.format("%s - No builder is available? Unable to send back %s",
                      this.ts, TransactionInitResponse.class.getSimpleName()));
        }
    }
    
    @Override
    protected void abortCallback(Status status) {
        // Uh... this might have already been sent out?
        if (this.builder != null) {
            if (debug.get()) LOG.debug(String.format("%s - Aborting %s with status %s",
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
            this.clearCounter();
        }
        else if (debug.get()) {
            LOG.warn(String.format("%s - No builder is available? Unable to send back %s",
                      this.ts, TransactionInitResponse.class.getSimpleName()));
        }
    }
}
