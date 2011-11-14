package edu.mit.hstore.handlers;

import java.util.Collection;

import org.apache.log4j.Logger;

import ca.evanjones.protorpc.ProtoRpcController;

import com.google.protobuf.RpcCallback;
import com.google.protobuf.RpcController;

import edu.brown.hstore.Hstore;
import edu.brown.hstore.Hstore.HStoreService;
import edu.brown.hstore.Hstore.TransactionFinishRequest;
import edu.brown.hstore.Hstore.TransactionFinishResponse;
import edu.brown.logging.LoggerUtil;
import edu.brown.logging.LoggerUtil.LoggerBoolean;
import edu.mit.hstore.HStoreCoordinator;
import edu.mit.hstore.HStoreSite;
import edu.mit.hstore.HStoreCoordinator.Dispatcher;
import edu.mit.hstore.dtxn.LocalTransaction;

public class TransactionFinishHandler extends AbstractTransactionHandler<TransactionFinishRequest, TransactionFinishResponse> {
    private static final Logger LOG = Logger.getLogger(TransactionFinishHandler.class);
    private static final LoggerBoolean debug = new LoggerBoolean(LOG.isDebugEnabled());
    private static final LoggerBoolean trace = new LoggerBoolean(LOG.isTraceEnabled());
    static {
        LoggerUtil.attachObserver(LOG, debug, trace);
    }
    
    final Dispatcher<Object[]> finishDispatcher; 
    
    public TransactionFinishHandler(HStoreSite hstore_site, HStoreCoordinator hstore_coord, Dispatcher<Object[]> finishDispatcher) {
        super(hstore_site, hstore_coord);
        this.finishDispatcher = finishDispatcher;
    }
    
    @Override
    public void sendLocal(long txn_id, TransactionFinishRequest request, Collection<Integer> partitions, RpcCallback<TransactionFinishResponse> callback) {
        hstore_site.transactionFinish(txn_id, request.getStatus(), partitions);
    }
    @Override
    public void sendRemote(HStoreService channel, ProtoRpcController controller, TransactionFinishRequest request, RpcCallback<TransactionFinishResponse> callback) {
        channel.transactionFinish(controller, request, callback);
    }
    @Override
    public void remoteQueue(RpcController controller, TransactionFinishRequest request, 
            RpcCallback<TransactionFinishResponse> callback) {
        if (finishDispatcher != null && request.getStatus() == Hstore.Status.ABORT_RESTART) {
            if (debug.get())
                LOG.debug("__FILE__:__LINE__ " + String.format("Queuing %s for txn #%d [status=%s]",
                                        request.getClass().getSimpleName(), request.getTransactionId(), request.getStatus()));
            Object o[] = { controller, request, callback };
            finishDispatcher.queue(o);
        } else {
            if (debug.get())
                LOG.debug("__FILE__:__LINE__ " + String.format("Sending %s to remote handler for txn #%d [status=%s]",
                                        request.getClass().getSimpleName(), request.getTransactionId(), request.getStatus()));
            this.remoteHandler(controller, request, callback);
        }
    }
    @Override
    public void remoteHandler(RpcController controller, TransactionFinishRequest request,
            RpcCallback<TransactionFinishResponse> callback) {
        assert(request.hasTransactionId()) : "Got Hstore." + request.getClass().getSimpleName() + " without a txn id!";
        long txn_id = request.getTransactionId();
        if (debug.get())
            LOG.debug("__FILE__:__LINE__ " + String.format("Got %s for txn #%d [status=%s]",
                                    request.getClass().getSimpleName(), txn_id, request.getStatus()));
        
        hstore_site.transactionFinish(txn_id, request.getStatus(), request.getPartitionsList());
        
        // Send back a FinishResponse to let them know we're cool with everything...
        Hstore.TransactionFinishResponse.Builder builder = Hstore.TransactionFinishResponse.newBuilder()
                                                          .setTransactionId(txn_id);
        Collection<Integer> local_partitions = hstore_site.getLocalPartitionIds();
        for (Integer p : request.getPartitionsList()) {
            if (local_partitions.contains(p)) builder.addPartitions(p.intValue());
        } // FOR
        if (debug.get())
            LOG.debug("__FILE__:__LINE__ " + String.format("Sending back %s for txn #%d [status=%s, partitions=%s]",
                                    TransactionFinishResponse.class.getSimpleName(), txn_id,
                                    request.getStatus(), builder.getPartitionsList()));
        callback.run(builder.build());
        
        // Always tell the HStoreSite to clean-up any state for this txn
        // hstore_site.completeTransaction(txn_id, request.getStatus());
        
    }
    @Override
    protected ProtoRpcController getProtoRpcController(LocalTransaction ts, int site_id) {
        return ts.getTransactionFinishController(site_id);
    }

}
