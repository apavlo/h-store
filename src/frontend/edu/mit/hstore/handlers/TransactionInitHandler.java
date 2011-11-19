package edu.mit.hstore.handlers;

import java.util.Collection;

import org.apache.log4j.Logger;

import ca.evanjones.protorpc.ProtoRpcController;

import com.google.protobuf.RpcCallback;
import com.google.protobuf.RpcController;

import edu.brown.hstore.Hstore.HStoreService;
import edu.brown.hstore.Hstore.TransactionInitRequest;
import edu.brown.hstore.Hstore.TransactionInitResponse;
import edu.brown.logging.LoggerUtil;
import edu.brown.logging.LoggerUtil.LoggerBoolean;
import edu.mit.hstore.HStoreCoordinator;
import edu.mit.hstore.HStoreObjectPools;
import edu.mit.hstore.HStoreSite;
import edu.mit.hstore.HStoreCoordinator.Dispatcher;
import edu.mit.hstore.callbacks.TransactionInitWrapperCallback;
import edu.mit.hstore.dtxn.AbstractTransaction;
import edu.mit.hstore.dtxn.LocalTransaction;

public class TransactionInitHandler extends AbstractTransactionHandler<TransactionInitRequest, TransactionInitResponse> {
    private static final Logger LOG = Logger.getLogger(TransactionInitHandler.class);
    private static final LoggerBoolean debug = new LoggerBoolean(LOG.isDebugEnabled());
    private static final LoggerBoolean trace = new LoggerBoolean(LOG.isTraceEnabled());
    static {
        LoggerUtil.attachObserver(LOG, debug, trace);
    }
    
    private final Dispatcher<Object[]> initDispatcher;
    
    public TransactionInitHandler(HStoreSite hstore_site, HStoreCoordinator hstore_coord, Dispatcher<Object[]> initDispatcher) {
        super(hstore_site, hstore_coord);
        this.initDispatcher = initDispatcher;
    }
    
    @Override
    public void sendLocal(long txn_id, TransactionInitRequest request, Collection<Integer> partitions, RpcCallback<TransactionInitResponse> callback) {
        handler.transactionInit(null, request, callback);
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
        assert(request.hasTransactionId()) : "Got Hstore." + request.getClass().getSimpleName() + " without a txn id!";
        long txn_id = request.getTransactionId();
        if (debug.get())
            LOG.debug(String.format("Got %s for txn #%d", request.getClass().getSimpleName(), txn_id));
        
        AbstractTransaction ts = hstore_site.getTransaction(txn_id); 
        assert(ts == null || ts instanceof LocalTransaction) :
            String.format("Got init request for remote txn #%d but we already have one [%s]",
                          txn_id, ts);
        
        // Wrap the callback around a TransactionInitWrapperCallback that will wait until
        // our HStoreSite gets an acknowledgment from all the
        // TODO: Figure out how we're going to return this callback to its ObjectPool
        TransactionInitWrapperCallback wrapper = null;
        try {
            wrapper = HStoreObjectPools.CALLBACKS_TXN_INITWRAPPER.borrowObject();
            wrapper.init(txn_id, request.getPartitionsList(), callback);
        } catch (Exception ex) {
            throw new RuntimeException(ex);
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
