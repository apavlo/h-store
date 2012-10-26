package edu.brown.hstore.callbacks;

import java.io.IOException;
import java.util.Collection;
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
import edu.brown.hstore.txns.AbstractTransaction;
import edu.brown.logging.LoggerUtil;
import edu.brown.logging.LoggerUtil.LoggerBoolean;
import edu.brown.profilers.PartitionExecutorProfiler;
import edu.brown.utils.PartitionSet;

/**
 * This callback is used for when a transaction is waiting in the TransactionQueueManager at this site 
 * Only when we get all the acknowledgments (through the run method) for the local partitions at 
 * this HStoreSite will we invoke the original callback. It is used as a wrapper around
 * the RpcCallback created by the ProtoRPC framework.
 * @author pavlo
 */
public class TransactionInitQueueCallback extends BlockingRpcCallback<TransactionInitResponse, Integer> {
    private static final Logger LOG = Logger.getLogger(TransactionInitQueueCallback.class);
    private static final LoggerBoolean debug = new LoggerBoolean(LOG.isDebugEnabled());
    private static final LoggerBoolean trace = new LoggerBoolean(LOG.isTraceEnabled());
    static {
        LoggerUtil.attachObserver(LOG, debug, trace);
    }
            
    private TransactionInitResponse.Builder builder = null;
    private PartitionSet partitions = null;
    private final boolean prefetch; 
    private final FastDeserializer fd = new FastDeserializer(new byte[0]);
    
    // ----------------------------------------------------------------------------
    // INTIALIZATION
    // ----------------------------------------------------------------------------
    
    public TransactionInitQueueCallback(HStoreSite hstore_site) {
        super(hstore_site, false);
        this.prefetch = hstore_site.getHStoreConf().site.exec_prefetch_queries;
    }
    
    public void init(Long txn_id,
                     PartitionSet partitions,
                     RpcCallback<TransactionInitResponse> orig_callback) {
        if (debug.get())
            LOG.debug(String.format("Starting new %s for txn #%d", this.getClass().getSimpleName(), txn_id));
        assert(orig_callback != null) :
            String.format("Tried to initialize %s with a null callback for txn #%d", this.getClass().getSimpleName(), txn_id);
        assert(partitions != null) :
            String.format("Tried to initialize %s with a null partitions for txn #%d", this.getClass().getSimpleName(), txn_id);
        
        // Only include local partitions
        int counter = 0;
        for (Integer p : this.hstore_site.getLocalPartitionIdArray()) { // One less iterator :-)
            if (partitions.contains(p.intValue())) counter++;
        } // FOR
        assert(counter > 0) : String.format("InitPartitions:%s / LocalPartitions:%s", 
                                            partitions, this.hstore_site.getLocalPartitionIds());
        this.partitions = partitions;
        this.builder = TransactionInitResponse.newBuilder()
                             .setTransactionId(txn_id.longValue())
                             .setStatus(Status.OK);
        super.init(txn_id, counter, orig_callback);
    }
    
    /**
     * All of the partitions that this transaction needs (including remote)
     * @return
     */
    public Collection<Integer> getPartitions() {
        return (this.partitions);
    }
    
    @Override
    protected void finishImpl() {
        if (debug.get()) LOG.debug(String.format("Txn #%d - Clearing out %s",
                                   this.getTransactionId(), this.builder.getClass().getSimpleName()));
        this.builder = null;
    }
    
    @Override
    public boolean isInitialized() {
        return (this.builder != null && super.isInitialized());
    }
    
    @Override
    public void unblockCallback() {
        if (debug.get()) LOG.debug(String.format("Txn #%d - Checking whether we can send back %s with status %s",
                                   this.getTransactionId(),
                                   TransactionInitResponse.class.getSimpleName(),
                                   (this.builder != null ? this.builder.getStatus() : "???")));
        if (this.builder != null) {
            if (debug.get()) {
                LOG.debug(String.format("Txn #%d - Sending %s to %s with status %s",
                                        this.getTransactionId(),
                                        TransactionInitResponse.class.getSimpleName(),
                                        this.getOrigCallback().getClass().getSimpleName(),
                                        this.builder.getStatus()));
            }
            assert(this.builder.getPartitionsList() != null) :
                String.format("The %s for txn #%d has no results but it was suppose to have %d.",
                              builder.getClass().getSimpleName(), this.getTransactionId(), this.getOrigCounter());
            assert(this.getOrigCounter() == this.builder.getPartitionsCount()) :
                String.format("The %s for txn #%d has results from %d partitions but it was suppose to have %d.",
                              builder.getClass().getSimpleName(), this.getTransactionId(), builder.getPartitionsCount(), this.getOrigCounter());
            assert(this.getOrigCallback() != null) :
                String.format("The original callback for txn #%d is null!", this.getTransactionId());
            this.getOrigCallback().run(this.builder.build());
            this.builder = null;
            
            // start profile idle_waiting_dtxn_time on remote paritions
            if (this.hstore_conf.site.exec_profiling) {
                for (Integer p : this.hstore_site.getLocalPartitionIdArray()) {
                    if (this.partitions.contains(p.intValue())) {
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
            if (this.prefetch) {
                AbstractTransaction ts = hstore_site.getTransaction(this.txn_id);
                if (ts != null && ts.hasPrefetchQueries()) {
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
                            throw new ServerFaultException(msg, ex, this.txn_id);
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
        }
        else if (debug.get()) {
            LOG.debug(String.format("Txn #%d - No builder is available? Unable to send back %s",
                      this.getTransactionId(), TransactionInitResponse.class.getSimpleName()));
        }
    }
    
    /**
     * Special abort method for specifying the partition that rejected
     * this transaction and what the larger transaction id was that caused our
     * transaction to get rejected.
     * @param status
     * @param partition
     * @param txn_id
     */
    public void abort(Status status, int partition, Long txn_id) {
        if (this.builder != null) {
            if (debug.get()) LOG.debug(String.format("Txn #%d - Setting abort status to %s",
                                       this.getTransactionId(), status));
            if (txn_id != null) {
                this.builder.setRejectPartition(partition);
                this.builder.setRejectTransactionId(txn_id);
            }
            this.abort(status);
        }
    }
    
    @Override
    protected void abortCallback(Status status) {
        // Uh... this might have already been sent out?
        if (this.builder != null) {
            if (debug.get()) LOG.debug(String.format("Txn #%d - Aborting %s with status %s",
                                       this.getTransactionId(), this.getClass().getSimpleName(), status));
            
            // Ok so where's what going on here. We need to send back
            // an abort message, so we're going use the builder that we've been 
            // working on and send out the bomb back to the base partition tells it that this
            // transaction is kaput at this HStoreSite.
            this.builder.setStatus(status);
            this.builder.clearPartitions();
            for (Integer p : this.hstore_site.getLocalPartitionIdArray()) { // One less iterator :-)
                if (this.partitions.contains(p.intValue())) this.builder.addPartitions(p.intValue());
            } // FOR
            this.getOrigCallback().run(this.builder.build());
            this.builder = null;
        }
    }
    
    @Override
    protected synchronized int runImpl(Integer partition) {
        if (this.builder == null) return (1);
        
        assert(this.builder != null) :
            "Unexpected null TransactionInitResponse builder for txn #" + this.getTransactionId();
        assert(this.isAborted() == false) :
            "Trying to add partitions for txn #" + this.getTransactionId() + " after the callback has been aborted";
        this.builder.addPartitions(partition.intValue());
        return 1;
    }
}
