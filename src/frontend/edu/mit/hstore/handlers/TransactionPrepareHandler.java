package edu.mit.hstore.handlers;

import java.util.Collection;
import java.util.HashSet;

import org.apache.log4j.Logger;

import ca.evanjones.protorpc.ProtoRpcController;

import com.google.protobuf.RpcCallback;
import com.google.protobuf.RpcController;

import edu.brown.hstore.Hstore;
import edu.brown.hstore.Hstore.HStoreService;
import edu.brown.hstore.Hstore.TransactionPrepareRequest;
import edu.brown.hstore.Hstore.TransactionPrepareResponse;
import edu.brown.logging.LoggerUtil;
import edu.brown.logging.LoggerUtil.LoggerBoolean;
import edu.mit.hstore.HStoreCoordinator;
import edu.mit.hstore.HStoreSite;
import edu.mit.hstore.dtxn.LocalTransaction;

public class TransactionPrepareHandler extends AbstractTransactionHandler<TransactionPrepareRequest, TransactionPrepareResponse> {
    private static final Logger LOG = Logger.getLogger(TransactionPrepareHandler.class);
    private static final LoggerBoolean debug = new LoggerBoolean(LOG.isDebugEnabled());
    private static final LoggerBoolean trace = new LoggerBoolean(LOG.isTraceEnabled());
    static {
        LoggerUtil.attachObserver(LOG, debug, trace);
    }
    
    public TransactionPrepareHandler(HStoreSite hstore_site, HStoreCoordinator hstore_coord) {
        super(hstore_site, hstore_coord);
    }
    
    @Override
    public void sendLocal(long txn_id, TransactionPrepareRequest request, Collection<Integer> partitions, RpcCallback<TransactionPrepareResponse> callback) {
        // We don't care whether we actually updated anybody locally, so we don't need to
        // pass in a set to get the partitions that were updated here.
        hstore_site.transactionPrepare(txn_id, partitions, null);
    }
    @Override
    public void sendRemote(HStoreService channel, ProtoRpcController controller, TransactionPrepareRequest request, RpcCallback<TransactionPrepareResponse> callback) {
        channel.transactionPrepare(controller, request, callback);
    }
    @Override
    public void remoteQueue(RpcController controller, TransactionPrepareRequest request, 
            RpcCallback<TransactionPrepareResponse> callback) {
        if (debug.get())
            LOG.debug("__FILE__:__LINE__ " + String.format("Sending %s to remote handler for txn #%d",
                                    request.getClass().getSimpleName(), request.getTransactionId()));
        this.remoteHandler(controller, request, callback);
    }
    @Override
    public void remoteHandler(RpcController controller, TransactionPrepareRequest request,
            RpcCallback<TransactionPrepareResponse> callback) {
        assert(request.hasTransactionId()) : "Got Hstore." + request.getClass().getSimpleName() + " without a txn id!";
        long txn_id = request.getTransactionId();
        if (debug.get())
            LOG.debug("__FILE__:__LINE__ " + String.format("Got %s for txn #%d", request.getClass().getSimpleName(), txn_id));
        
        Collection<Integer> updated = new HashSet<Integer>();
        hstore_site.transactionPrepare(txn_id, request.getPartitionsList(), updated);
        assert(updated.isEmpty() == false);
        
        if (debug.get())
            LOG.debug("__FILE__:__LINE__ " + String.format("Finished PREPARE phase for txn #%d [updatedPartitions=%s]", txn_id, updated));
        Hstore.TransactionPrepareResponse response = Hstore.TransactionPrepareResponse.newBuilder()
                                                               .setTransactionId(txn_id)
                                                               .addAllPartitions(updated)
                                                               .setStatus(Hstore.Status.OK)
                                                               .build();
        callback.run(response);
    }
    @Override
    protected ProtoRpcController getProtoRpcController(LocalTransaction ts, int site_id) {
        return ts.getTransactionPrepareController(site_id);
    }
}
