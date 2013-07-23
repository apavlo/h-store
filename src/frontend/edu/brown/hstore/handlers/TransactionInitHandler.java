package edu.brown.hstore.handlers;

import org.apache.log4j.Logger;
import org.voltdb.ParameterSet;
import org.voltdb.exceptions.ServerFaultException;
import org.voltdb.messaging.FastDeserializer;

import com.google.protobuf.RpcCallback;
import com.google.protobuf.RpcController;

import edu.brown.hstore.HStoreCoordinator;
import edu.brown.hstore.HStoreSite;
import edu.brown.hstore.Hstoreservice.HStoreService;
import edu.brown.hstore.Hstoreservice.TransactionInitRequest;
import edu.brown.hstore.Hstoreservice.TransactionInitResponse;
import edu.brown.hstore.Hstoreservice.WorkFragment;
import edu.brown.hstore.callbacks.RemoteInitQueueCallback;
import edu.brown.hstore.dispatchers.AbstractDispatcher;
import edu.brown.hstore.txns.AbstractTransaction;
import edu.brown.hstore.txns.LocalTransaction;
import edu.brown.hstore.txns.RemoteTransaction;
import edu.brown.logging.LoggerUtil;
import edu.brown.logging.LoggerUtil.LoggerBoolean;
import edu.brown.protorpc.ProtoRpcController;
import edu.brown.utils.PartitionSet;

/**
 * Add the given transaction id to this site's queue manager with all of the partitions that
 * it needs to lock. This is only for distributed transactions.
 * The callback will be invoked once the transaction has acquired all of the locks for the
 * partitions provided, or aborted if the transaction is unable to lock those partitions.
 * @author pavlo
 */
public class TransactionInitHandler extends AbstractTransactionHandler<TransactionInitRequest, TransactionInitResponse> {
    private static final Logger LOG = Logger.getLogger(TransactionInitHandler.class);
    private static final LoggerBoolean debug = new LoggerBoolean();
    private static final LoggerBoolean trace = new LoggerBoolean();
    static {
        LoggerUtil.attachObserver(LOG, debug, trace);
    }
    
    private final AbstractDispatcher<Object[]> initDispatcher;
    
    public TransactionInitHandler(HStoreSite hstore_site, HStoreCoordinator hstore_coord, AbstractDispatcher<Object[]> initDispatcher) {
        super(hstore_site, hstore_coord);
        this.initDispatcher = initDispatcher;
    }
    
    @Override
    public void sendLocal(Long txn_id, TransactionInitRequest request, PartitionSet partitions, RpcCallback<TransactionInitResponse> callback) {
        this.remoteQueue(null, request, callback);
    }
    @Override
    public void sendRemote(HStoreService channel, ProtoRpcController controller, TransactionInitRequest request, RpcCallback<TransactionInitResponse> callback) {
        channel.transactionInit(controller, request, callback);
    }
    @Override
    public void remoteQueue(RpcController controller, TransactionInitRequest request,
            RpcCallback<TransactionInitResponse> callback) {
        if (initDispatcher != null) {
            if (debug.val) LOG.debug("Queuing request for txn #" + request.getTransactionId());
            Object o[] = { controller, request, callback };
            initDispatcher.queue(o);
        } else {
            this.remoteHandler(controller, request, callback);
        }
    }
    @Override
    public void remoteHandler(RpcController controller, TransactionInitRequest request, RpcCallback<TransactionInitResponse> callback) {
        assert(request.hasTransactionId()) : "Got " + request.getClass().getSimpleName() + " without a txn id!";
        Long txn_id = request.getTransactionId();
        if (debug.val)
            LOG.debug(String.format("Got %s for txn #%d", request.getClass().getSimpleName(), txn_id));
        
        AbstractTransaction ts = this.hstore_site.getTransaction(txn_id); 
        assert(ts == null || ts instanceof LocalTransaction) :
            String.format("Got init request for remote txn #%d but we already have one [%s]",
                          txn_id, ts);

        // This allocation is unnecessary if we're on the same site
        PartitionSet partitions = null;
        if (ts instanceof LocalTransaction) {
            partitions = ((LocalTransaction)ts).getPredictTouchedPartitions();
        } else {
            
            // We first need all of the partitions so that we know
            // what it's actually going to touch
            // The init callback obviously only needs to have the
            // partitions that are local at this site.
            partitions = new PartitionSet(request.getPartitionsList());

            ParameterSet procParams = null;
            if (request.hasProcParams()) {
                FastDeserializer fds = new FastDeserializer(request.getProcParams().asReadOnlyByteBuffer());
                try {
                    procParams = fds.readObject(ParameterSet.class);
                } catch (Exception ex) {
                    String msg = String.format("Failed to deserialize procedure ParameterSet for txn #%d from %s",
                                               txn_id, request.getClass().getSimpleName()); 
                    throw new ServerFaultException(msg, ex, txn_id);
                }
            }
            
            // If we don't have a handle, we need to make one so that we can stick in the
            // things that we need to keep track of at this site. At this point we know that we're on
            // a remote site from the txn's base partition
            ts = this.hstore_site.getTransactionInitializer()
                                 .createRemoteTransaction(txn_id,
                                                          partitions,
                                                          procParams,
                                                          request.getBasePartition(),
                                                          request.getProcedureId());
            
            // Make sure that we initialize the RemoteTransactionInitCallback too!
            RemoteInitQueueCallback initCallback = ts.getInitCallback();
            initCallback.init((RemoteTransaction)ts, partitions, callback);
        }
        
        // If (request.getPrefetchFragmentsCount() > 0), then we need to
        // make a RemoteTransaction handle for ourselves so that we can keep track of 
        // our state when prefetching queries.
        if (request.getPrefetchFragmentsCount() > 0) {
            // Stick the prefetch information into the transaction
            if (debug.val) {
                PartitionSet prefetchPartitions = new PartitionSet();
                for (WorkFragment fragment : request.getPrefetchFragmentsList())
                    prefetchPartitions.add(fragment.getPartitionId());
                LOG.debug(String.format("%s - Attaching %d prefetch %s at partitions %s",
                          ts, request.getPrefetchFragmentsCount(),
                          WorkFragment.class.getSimpleName(), prefetchPartitions));
            }
//            for (int i = 0; i < request.getPrefetchParamsCount(); i++) {
//                LOG.info(String.format("%s - XXX INBOUND PREFETCH RAW [%02d]: %s",
//                         ts, i,
//                         StringUtil.md5sum(request.getPrefetchParams(i).asReadOnlyByteBuffer())));
//            }
            
            ts.initializePrefetch();
            ts.attachPrefetchQueries(request.getPrefetchFragmentsList(),
                                     request.getPrefetchParamsList());
        }

        // We don't need to send back a response right here.
        // The init callback will wait until it has results from all of the partitions 
        // the tasks were sent to and then send back everything in a single response message
        this.hstore_site.transactionInit(ts);
    }
    @Override
    protected ProtoRpcController getProtoRpcController(LocalTransaction ts, int site_id) {
        return ts.getTransactionInitController(site_id);
    }
}
