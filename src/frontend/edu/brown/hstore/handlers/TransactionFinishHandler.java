package edu.brown.hstore.handlers;

import org.apache.log4j.Logger;

import com.google.protobuf.RpcCallback;
import com.google.protobuf.RpcController;

import edu.brown.hstore.HStoreCoordinator;
import edu.brown.hstore.HStoreSite;
import edu.brown.hstore.Hstoreservice.HStoreService;
import edu.brown.hstore.Hstoreservice.Status;
import edu.brown.hstore.Hstoreservice.TransactionFinishRequest;
import edu.brown.hstore.Hstoreservice.TransactionFinishResponse;
import edu.brown.hstore.callbacks.PartitionCountingCallback;
import edu.brown.hstore.dispatchers.AbstractDispatcher;
import edu.brown.hstore.txns.AbstractTransaction;
import edu.brown.hstore.txns.LocalTransaction;
import edu.brown.logging.LoggerUtil;
import edu.brown.logging.LoggerUtil.LoggerBoolean;
import edu.brown.protorpc.ProtoRpcController;
import edu.brown.utils.PartitionSet;

public class TransactionFinishHandler extends AbstractTransactionHandler<TransactionFinishRequest, TransactionFinishResponse> {
    private static final Logger LOG = Logger.getLogger(TransactionFinishHandler.class);
    private static final LoggerBoolean debug = new LoggerBoolean();
    static {
        LoggerUtil.attachObserver(LOG, debug);
    }
    
    final AbstractDispatcher<Object[]> finishDispatcher;
    
    // Reusable container for the partitions that we need to
    // use to tell the HStoreSite that we're finished with
    // This is thread-safe
    final PartitionSet finishPartitions = new PartitionSet();
    
    public TransactionFinishHandler(HStoreSite hstore_site, HStoreCoordinator hstore_coord, AbstractDispatcher<Object[]> finishDispatcher) {
        super(hstore_site, hstore_coord);
        this.finishDispatcher = finishDispatcher;
    }
    
    @Override
    public void sendLocal(Long txn_id, TransactionFinishRequest request, PartitionSet partitions,
                          RpcCallback<TransactionFinishResponse> callback) {
        this.hstore_site.transactionFinish(txn_id, request.getStatus(), partitions);
    }
    @Override
    public void sendRemote(HStoreService channel, ProtoRpcController controller, TransactionFinishRequest request,
                           RpcCallback<TransactionFinishResponse> callback) {
        channel.transactionFinish(controller, request, callback);
    }
    @Override
    public void remoteQueue(RpcController controller, TransactionFinishRequest request,
                            RpcCallback<TransactionFinishResponse> callback) {
        if (this.finishDispatcher != null && request.getStatus() == Status.ABORT_RESTART) {
            if (debug.val)
                LOG.debug(String.format("Queuing %s for txn #%d [status=%s]",
                          request.getClass().getSimpleName(), request.getTransactionId(), request.getStatus()));
            Object o[] = { controller, request, callback };
            this.finishDispatcher.queue(o);
        } else {
            if (debug.val)
                LOG.debug(String.format("Sending %s to remote handler for txn #%d [status=%s]",
                          request.getClass().getSimpleName(), request.getTransactionId(), request.getStatus()));
            this.remoteHandler(controller, request, callback);
        }
    }
    @Override
    public void remoteHandler(RpcController controller, TransactionFinishRequest request,
                              RpcCallback<TransactionFinishResponse> callback) {
        assert(request.hasTransactionId()) : "Got " + request.getClass().getSimpleName() + " without a txn id!";
        Long txn_id = Long.valueOf(request.getTransactionId());
        if (debug.val)
            LOG.debug(String.format("Got %s for txn #%d [status=%s]",
                      request.getClass().getSimpleName(), txn_id, request.getStatus()));

        // Cancel the InitCallback if it hasn't been invoked yet
        AbstractTransaction ts = this.hstore_site.getTransaction(txn_id);
        if (ts != null) {
            PartitionCountingCallback<AbstractTransaction> initCallback = ts.getInitCallback();
            if (initCallback.isUnblocked() == false && initCallback.isAborted() == false) {
                initCallback.cancel();
            }
        }
        
        this.finishPartitions.clear();
        this.finishPartitions.addAll(request.getPartitionsList());
        this.hstore_site.transactionFinish(txn_id, request.getStatus(), this.finishPartitions);
        
        // Send back a FinishResponse to let them know we're cool with everything...
        TransactionFinishResponse.Builder builder = TransactionFinishResponse.newBuilder()
                                                          .setTransactionId(txn_id);
        for (int p : request.getPartitionsList()) {
            if (hstore_site.isLocalPartition(p)) builder.addPartitions(p);
        } // FOR
        if (debug.val)
            LOG.debug(String.format("Sending back %s for txn #%d [status=%s, partitions=%s]",
                      TransactionFinishResponse.class.getSimpleName(), txn_id,
                      request.getStatus(), builder.getPartitionsList()));
        callback.run(builder.build());
    }
    @Override
    protected ProtoRpcController getProtoRpcController(LocalTransaction ts, int site_id) {
        return ts.getTransactionFinishController(site_id);
    }

}
