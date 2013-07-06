package edu.brown.hstore.handlers;

import org.apache.log4j.Logger;

import com.google.protobuf.RpcCallback;
import com.google.protobuf.RpcController;

import edu.brown.hstore.HStoreCoordinator;
import edu.brown.hstore.HStoreSite;
import edu.brown.hstore.Hstoreservice.HStoreService;
import edu.brown.hstore.Hstoreservice.TransactionPrepareRequest;
import edu.brown.hstore.Hstoreservice.TransactionPrepareResponse;
import edu.brown.hstore.callbacks.RemotePrepareCallback;
import edu.brown.hstore.txns.LocalTransaction;
import edu.brown.hstore.txns.RemoteTransaction;
import edu.brown.logging.LoggerUtil;
import edu.brown.logging.LoggerUtil.LoggerBoolean;
import edu.brown.protorpc.ProtoRpcController;
import edu.brown.utils.PartitionSet;

public class TransactionPrepareHandler extends AbstractTransactionHandler<TransactionPrepareRequest, TransactionPrepareResponse> {
    private static final Logger LOG = Logger.getLogger(TransactionPrepareHandler.class);
    private static final LoggerBoolean debug = new LoggerBoolean();
    static {
        LoggerUtil.attachObserver(LOG, debug);
    }
    
    public TransactionPrepareHandler(HStoreSite hstore_site, HStoreCoordinator hstore_coord) {
        super(hstore_site, hstore_coord);
    }
    
    @Override
    public void sendLocal(Long txn_id, TransactionPrepareRequest request, PartitionSet partitions, RpcCallback<TransactionPrepareResponse> callback) {
        // We don't care whether we actually updated anybody locally, so we don't need to
        // pass in a set to get the partitions that were updated here.
        LocalTransaction ts = this.hstore_site.getTransaction(txn_id);
        assert(ts != null) : "Unexpected null transaction handle for txn #" + txn_id;
        this.hstore_site.transactionPrepare(ts, partitions, ts.getPrepareCallback());
    }
    @Override
    public void sendRemote(HStoreService channel, ProtoRpcController controller, TransactionPrepareRequest request, RpcCallback<TransactionPrepareResponse> callback) {
        channel.transactionPrepare(controller, request, callback);
    }
    @Override
    public void remoteQueue(RpcController controller, TransactionPrepareRequest request, 
            RpcCallback<TransactionPrepareResponse> callback) {
        if (debug.val)
            LOG.debug(String.format("Sending %s to remote handler for txn #%d",
                      request.getClass().getSimpleName(), request.getTransactionId()));
        this.remoteHandler(controller, request, callback);
    }
    @Override
    public void remoteHandler(RpcController controller, TransactionPrepareRequest request,
            RpcCallback<TransactionPrepareResponse> callback) {
        assert(request.hasTransactionId()) :
            "Got " + request.getClass().getSimpleName() + " without a txn id!";
        Long txn_id = Long.valueOf(request.getTransactionId());
        if (debug.val)
            LOG.debug(String.format("Got %s for txn #%d", request.getClass().getSimpleName(), txn_id));
        
        // HACK
        // Use a TransactionPrepareWrapperCallback to ensure that we only send back
        // the prepare response once all of the PartitionExecutors have successfully
        // acknowledged that we're ready to commit
        PartitionSet partitions = new PartitionSet(request.getPartitionsList());
        assert(partitions.isEmpty() == false) :
            "Unexpected empty list of updated partitions for txn #" + txn_id;
        partitions.retainAll(hstore_site.getLocalPartitionIds());
        
        RemoteTransaction ts = this.hstore_site.getTransaction(txn_id);
        assert(ts != null) : "Unexpected null transaction handle for txn #" + txn_id;
        
        // Always create a new prepare callback because we may get multiple messages
        // to prepare this txn for commit. 
        RemotePrepareCallback wrapper = ts.getPrepareCallback();
        assert(wrapper.isInitialized() == false);
        wrapper.init(ts, partitions, callback);
        
        this.hstore_site.transactionPrepare(ts, partitions, wrapper);
    }
    @Override
    protected ProtoRpcController getProtoRpcController(LocalTransaction ts, int site_id) {
        return ts.getTransactionPrepareController(site_id);
    }
}