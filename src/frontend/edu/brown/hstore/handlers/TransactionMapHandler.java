package edu.brown.hstore.handlers;

import org.apache.log4j.Logger;
import org.voltdb.utils.EstTime;

import com.google.protobuf.RpcCallback;
import com.google.protobuf.RpcController;

import edu.brown.hstore.HStoreCoordinator;
import edu.brown.hstore.HStoreSite;
import edu.brown.hstore.Hstoreservice.HStoreService;
import edu.brown.hstore.Hstoreservice.TransactionMapRequest;
import edu.brown.hstore.Hstoreservice.TransactionMapResponse;
import edu.brown.hstore.txns.LocalTransaction;
import edu.brown.hstore.txns.MapReduceTransaction;
import edu.brown.logging.LoggerUtil;
import edu.brown.logging.LoggerUtil.LoggerBoolean;
import edu.brown.protorpc.ProtoRpcController;
import edu.brown.utils.PartitionSet;

public class TransactionMapHandler extends AbstractTransactionHandler<TransactionMapRequest, TransactionMapResponse> {
    private static final Logger LOG = Logger.getLogger(TransactionMapHandler.class);
    private static final LoggerBoolean debug = new LoggerBoolean();
    private static final LoggerBoolean trace = new LoggerBoolean();
    static {
        LoggerUtil.attachObserver(LOG, debug, trace);
    }
    
    //final Dispatcher<Object[]> MapDispatcher;
    
    public TransactionMapHandler(HStoreSite hstore_site, HStoreCoordinator hstore_coord) {
        super(hstore_site, hstore_coord);
    }
    
    @Override
    public void sendLocal(Long txn_id, TransactionMapRequest request, PartitionSet partitions, RpcCallback<TransactionMapResponse> callback) {
        // This is for MapReduce Transaction, the local task is still passed to the remoteHandler to be invoked the TransactionStart
        // as the a LocalTransaction. This LocalTransaction in this partition is the base partition for MR transaction.
        if (debug.val) LOG.debug("Send to remoteHandler from sendLocal");
        this.remoteHandler(null, request, callback);
    }
    @Override
    public void sendRemote(HStoreService channel, ProtoRpcController controller, TransactionMapRequest request, RpcCallback<TransactionMapResponse> callback) {
        channel.transactionMap(controller, request, callback);
    }
    @Override
    public void remoteQueue(RpcController controller, TransactionMapRequest request,
            RpcCallback<TransactionMapResponse> callback) {
        this.remoteHandler(controller, request, callback);
    }
    @Override
    public void remoteHandler(RpcController controller,
                               TransactionMapRequest request,
                               RpcCallback<TransactionMapResponse> callback) {
        assert(request.hasTransactionId()) :
            "Got " + request.getClass().getSimpleName() + " without a txn id!";
        Long txn_id = Long.valueOf(request.getTransactionId());
        if (debug.val)
            LOG.debug(String.format("Got %s for txn #%d",
                                   request.getClass().getSimpleName(), txn_id));

        // The mr_ts handle will be null if this HStoreSite is not where the 
        // base partition for the original MRTransaction
        MapReduceTransaction mr_ts = hstore_site.getTransaction(txn_id);
        if (mr_ts == null) {
            mr_ts = hstore_site.getTransactionInitializer()
                               .createMapReduceTransaction(txn_id,
                                                           EstTime.currentTimeMillis(),
                                                           request.getClientHandle(),
                                                           request.getBasePartition(),
                                                           request.getProcedureId(),
                                                           request.getParams().asReadOnlyByteBuffer());
        }
        assert(mr_ts.isMapPhase());
        mr_ts.initTransactionMapWrapperCallback(callback);

        /*
         * Here we would like to start MapReduce Transaction on the remote partition except the base partition of it.
         * This is to avoid the double invoke for remote task. 
         * */
        for (int partition : hstore_site.getLocalPartitionIds()) {
            if (partition != mr_ts.getBasePartition()) { 
                LocalTransaction ts = mr_ts.getLocalTransaction(partition);
                hstore_site.transactionStart(ts);
            }
        } // FOR
    }
    @Override
    protected ProtoRpcController getProtoRpcController(LocalTransaction ts, int site_id) {
        return ts.getTransactionWorkController(site_id);
    }
}
