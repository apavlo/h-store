package edu.brown.hstore.callbacks;

import java.util.ArrayList;
import java.util.ConcurrentModificationException;
import java.util.List;

import org.apache.log4j.Logger;
import org.voltdb.messaging.FastDeserializer;

import com.google.protobuf.RpcCallback;

import edu.brown.hstore.HStoreSite;
import edu.brown.hstore.TransactionQueueManager;
import edu.brown.hstore.Hstoreservice.Status;
import edu.brown.hstore.Hstoreservice.TransactionInitResponse;
import edu.brown.hstore.specexec.PrefetchQueryUtil;
import edu.brown.hstore.txns.LocalTransaction;
import edu.brown.logging.LoggerUtil;
import edu.brown.logging.LoggerUtil.LoggerBoolean;
import edu.brown.utils.PartitionSet;
import edu.brown.utils.StringUtil;

/**
 * InitQueue Callback on the Txn's Local HStoreSite
 * @author pavlo
 */
public class LocalInitQueueCallback extends PartitionCountingCallback<LocalTransaction> implements RpcCallback<TransactionInitResponse> {
    private static final Logger LOG = Logger.getLogger(LocalInitQueueCallback.class);
    private static final LoggerBoolean debug = new LoggerBoolean();
    static {
        LoggerUtil.attachObserver(LOG, debug);
    }

    private final boolean prefetch;
    private ThreadLocal<FastDeserializer> prefetchDeserializers;
    private final TransactionQueueManager txnQueueManager;
    private final List<TransactionInitResponse> responses = new ArrayList<TransactionInitResponse>();
    
    // ----------------------------------------------------------------------------
    // INTIALIZATION
    // ----------------------------------------------------------------------------
    
    public LocalInitQueueCallback(HStoreSite hstore_site) {
        super(hstore_site);
        this.prefetch = hstore_site.getHStoreConf().site.exec_prefetch_queries;
        this.txnQueueManager = hstore_site.getTransactionQueueManager();
    }
    
    @Override
    public void init(LocalTransaction ts, PartitionSet partitions) {
        this.responses.clear();
        super.init(ts, partitions);
    }
    
    // ----------------------------------------------------------------------------
    // CALLBACK METHODS
    // ----------------------------------------------------------------------------

    @Override
    public void run(int partition) {
        if (debug.val)
            LOG.debug(String.format("%s - Prefetch=%s / HasPrefetchFragments=%s",
                      this.ts, this.prefetch, this.ts.hasPrefetchFragments()));
        if (this.prefetch && 
                this.ts.hasPrefetchFragments() &&
                partition != this.ts.getBasePartition() &&
                hstore_site.isLocalPartition(partition)) {
            if (debug.val)
                LOG.debug(String.format("%s - Checking for prefetch queries at partition %d",
                          this.ts, partition));
            if (this.prefetchDeserializers == null) {
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
        assert(this.isAborted() == false) :
            "Trying unblock " + this.ts + " but it was already marked as aborted";
        
        // HACK: If this is a single-partition txn, then we don't
        // need to submit it for execution because the PartitionExecutor
        // will fire it off right away
        if (this.ts.isPredictSinglePartition() == false) {
            if (debug.val) LOG.debug(this.ts + " is ready to execute. Passing to HStoreSite");
            this.hstore_site.transactionStart(this.ts);
        }
    }

    @Override
    protected void abortCallback(int partition, Status status) {
        // If the transaction needs to be restarted, then we'll attempt to requeue it.
        switch (status) {
            case ABORT_SPECULATIVE:
            case ABORT_RESTART:
                // We don't care whether our transaction was rejected or not because we 
                // know that we still need to call TransactionFinish, which will delete
                // the final transaction state
                this.txnQueueManager.restartTransaction(this.ts, status);
                break;
            case ABORT_REJECT:
                this.hstore_site.transactionReject(this.ts, status);
                break;
            default:
                throw new RuntimeException(String.format("Unexpected status %s for %s", status, this.ts));
        } // SWITCH
        // this.cancel();
    }
    
    // ----------------------------------------------------------------------------
    // RPC CALLBACK
    // ----------------------------------------------------------------------------

    @Override
    public void run(TransactionInitResponse response) {
        if (debug.val)
            LOG.debug(String.format("%s - Got %s with status %s from partitions %s",
                      this.ts, response.getClass().getSimpleName(),
                      response.getStatus(), response.getPartitionsList()));
        this.responses.add(response);
        if (response.getStatus() != Status.OK) {
            int reject_partition = response.getRejectPartition();
            for (int partition : response.getPartitionsList()) {
                if (partition != reject_partition) {
                    this.decrementCounter(partition);
                }
            } // FOR
            // The last one should call the actual abort
            this.abort(reject_partition, response.getStatus());        
        } else {
            for (Integer partition : response.getPartitionsList()) {
                this.run(partition.intValue());
            } // FOR
        }
    }
    
    @Override
    public String toString() {
        String ret = super.toString();
        if (this.responses.isEmpty() == false) {
            ret += "\n-------------\n";
            String debug = "";
            while (true) {
                try {
                    debug = StringUtil.join("\n", this.responses); 
                } catch (ConcurrentModificationException ex) {
                    continue;
                }
                break;
            }
            ret += debug;
        }
        return (ret);
    }
}
