package edu.brown.hstore.handlers;

import org.apache.log4j.Logger;

import com.google.protobuf.RpcCallback;
import com.google.protobuf.RpcController;

import edu.brown.hstore.HStoreCoordinator;
import edu.brown.hstore.HStoreSite;
import edu.brown.hstore.Hstoreservice;
import edu.brown.hstore.Hstoreservice.HStoreService;
import edu.brown.hstore.Hstoreservice.TransactionPrepareRequest;
import edu.brown.hstore.Hstoreservice.TransactionPrepareResponse;
import edu.brown.hstore.txns.LocalTransaction;
import edu.brown.logging.LoggerUtil;
import edu.brown.logging.LoggerUtil.LoggerBoolean;
import edu.brown.profilers.PartitionExecutorProfiler;
import edu.brown.protorpc.ProtoRpcController;
import edu.brown.utils.PartitionSet;

public class TransactionPrepareHandler extends AbstractTransactionHandler<TransactionPrepareRequest, TransactionPrepareResponse> {
    private static final Logger LOG = Logger.getLogger(TransactionPrepareHandler.class);
    private static final LoggerBoolean debug = new LoggerBoolean(LOG.isDebugEnabled());
    private static final LoggerBoolean trace = new LoggerBoolean(LOG.isTraceEnabled());
    static {
        LoggerUtil.attachObserver(LOG, debug, trace);
    }
    
    /**
     * XXX: I think this is thread safe
     */
    private final PartitionSet targetPartitions = new PartitionSet();
    private final PartitionSet updatedPartitions = new PartitionSet();
    
    public TransactionPrepareHandler(HStoreSite hstore_site, HStoreCoordinator hstore_coord) {
        super(hstore_site, hstore_coord);
    }
    
    @Override
    public void sendLocal(Long txn_id, TransactionPrepareRequest request, PartitionSet partitions, RpcCallback<TransactionPrepareResponse> callback) {
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
            LOG.debug(String.format("Sending %s to remote handler for txn #%d",
                                    request.getClass().getSimpleName(), request.getTransactionId()));
        this.remoteHandler(controller, request, callback);
    }
    @Override
    public void remoteHandler(RpcController controller, TransactionPrepareRequest request,
            RpcCallback<TransactionPrepareResponse> callback) {
        assert(request.hasTransactionId()) : "Got Hstore." + request.getClass().getSimpleName() + " without a txn id!";
        long txn_id = request.getTransactionId();
        if (debug.get())
            LOG.debug(String.format("Got %s for txn #%d", request.getClass().getSimpleName(), txn_id));
        
        // XXX: Check whether this thread safe. I think it is
        this.targetPartitions.clear();
        this.targetPartitions.addAll(request.getPartitionsList());
        this.updatedPartitions.clear();
        
        hstore_site.transactionPrepare(txn_id, this.targetPartitions, this.updatedPartitions);
        assert(this.updatedPartitions.isEmpty() == false) :
            "Unexpected empty list of updated partitions for txn #" + txn_id;
        
        for (int p: updatedPartitions) {
            if (this.hstore_site.getHStoreConf().site.exec_profiling) {
                PartitionExecutorProfiler pep = this.hstore_site.getPartitionExecutor(p).getProfiler();
                assert (pep != null);
                if (pep.idle_2pc_remote_time.isStarted()) pep.idle_2pc_remote_time.stop();
                pep.idle_2pc_remote_time.start();
            }
        }
        
        if (debug.get()) LOG.debug(String.format("Finished PREPARE phase for txn #%d [updatedPartitions=%s]",
                                                 txn_id, this.updatedPartitions));
        TransactionPrepareResponse response = TransactionPrepareResponse.newBuilder()
                                                               .setTransactionId(txn_id)
                                                               .addAllPartitions(this.updatedPartitions)
                                                               .setStatus(Hstoreservice.Status.OK)
                                                               .build();
        callback.run(response);
    }
    @Override
    protected ProtoRpcController getProtoRpcController(LocalTransaction ts, int site_id) {
        return ts.getTransactionPrepareController(site_id);
    }
}
