package edu.brown.hstore.handlers;

import java.util.Collection;

import org.apache.log4j.Logger;

import com.google.protobuf.RpcCallback;
import com.google.protobuf.RpcController;

import edu.brown.catalog.CatalogUtil;
import edu.brown.hstore.HStoreCoordinator;
import edu.brown.hstore.HStoreObjectPools;
import edu.brown.hstore.HStoreSite;
import edu.brown.hstore.Hstoreservice.HStoreService;
import edu.brown.hstore.Hstoreservice.TransactionInitRequest;
import edu.brown.hstore.Hstoreservice.TransactionInitResponse;
import edu.brown.hstore.callbacks.TransactionInitQueueCallback;
import edu.brown.hstore.dispatchers.AbstractDispatcher;
import edu.brown.hstore.dtxn.AbstractTransaction;
import edu.brown.hstore.dtxn.LocalTransaction;
import edu.brown.logging.LoggerUtil;
import edu.brown.logging.LoggerUtil.LoggerBoolean;
import edu.brown.protorpc.ProtoRpcController;

public class TransactionInitHandler extends AbstractTransactionHandler<TransactionInitRequest, TransactionInitResponse> {
    private static final Logger LOG = Logger.getLogger(TransactionInitHandler.class);
    private static final LoggerBoolean debug = new LoggerBoolean(LOG.isDebugEnabled());
    private static final LoggerBoolean trace = new LoggerBoolean(LOG.isTraceEnabled());
    static {
        LoggerUtil.attachObserver(LOG, debug, trace);
    }
    
    private final AbstractDispatcher<Object[]> initDispatcher;
    
    public TransactionInitHandler(HStoreSite hstore_site, HStoreCoordinator hstore_coord, AbstractDispatcher<Object[]> initDispatcher) {
        super(hstore_site, hstore_coord);
        this.initDispatcher = initDispatcher;
    }
    
    @Override
    public void sendLocal(Long txn_id, TransactionInitRequest request, Collection<Integer> partitions, RpcCallback<TransactionInitResponse> callback) {
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
            if (debug.get()) LOG.debug("Queuing request for txn #" + request.getTransactionId());
            Object o[] = { controller, request, callback };
            initDispatcher.queue(o);
        } else {
            this.remoteHandler(controller, request, callback);
        }
    }
    @Override
    public void remoteHandler(RpcController controller, TransactionInitRequest request,
            RpcCallback<TransactionInitResponse> callback) {
        assert(request.hasTransactionId()) : "Got " + request.getClass().getSimpleName() + " without a txn id!";
        Long txn_id = request.getTransactionId();
        if (debug.get())
            LOG.debug(String.format("Got %s for txn #%d", request.getClass().getSimpleName(), txn_id));
        
        AbstractTransaction ts = hstore_site.getTransaction(txn_id); 
        assert(ts == null || ts instanceof LocalTransaction) :
            String.format("Got init request for remote txn #%d but we already have one [%s]",
                          txn_id, ts);
        
        // Wrap the callback around a TransactionInitWrapperCallback that will wait until
        // our HStoreSite gets an acknowledgment from all the ...
        // Note: The TransactionQueueManager will put this back in the queue for us
        //       We have to allocate this here because we need to have the original callback
        TransactionInitQueueCallback wrapper = null;
        try {
            wrapper = HStoreObjectPools.CALLBACKS_TXN_INITQUEUE.borrowObject();
            wrapper.init(txn_id, request.getPartitionsList(), callback);
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
        
        // If (request.getPrefetchFragmentsCount() > 0), then we need to
        // make a RemoteTransaction handle for ourselves so that we can keep track of 
        // our state when pre-fetching queries.
        if (request.getPrefetchFragmentsCount() > 0) {
            // If we don't have a handle, we need to make one so that we can stick in the
            // things that we need to prefetch. At this point we know that we're on
            // a remote site from the txn's base partition
            if (ts == null) {
                int base_partition = request.getBasePartition();
                boolean sysproc = CatalogUtil.isSysProcedure(hstore_site.getDatabase(), request.getProcedureId());
                ts = hstore_site.createRemoteTransaction(txn_id, base_partition, sysproc);
            }
            
            // Stick the prefetch information into the transaction
            if (debug.get()) LOG.debug(String.format("%s - Attaching %d prefetch WorkFragments at %s",
                                                     ts, request.getPrefetchFragmentsCount(), hstore_site.getSiteName()));
            ts.initializePrefetch();
            ts.attachPrefetchQueries(request.getPrefetchFragmentsList(),
                                     request.getPrefetchParamsList());
        }
        
        
        hstore_site.transactionInit(txn_id, request.getPartitionsList(), wrapper);
        
        // We don't need to send back a response right here.
        // TransactionInitWrapperCallback will wait until it has results from all of the partitions 
        // the tasks were sent to and then send back everything in a single response message
    }
    @Override
    protected ProtoRpcController getProtoRpcController(LocalTransaction ts, int site_id) {
        return ts.getTransactionInitController(site_id);
    }
}
