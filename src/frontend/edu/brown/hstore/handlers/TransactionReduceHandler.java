package edu.brown.hstore.handlers;

import java.util.Collection;

import org.apache.log4j.Logger;

import com.google.protobuf.RpcCallback;
import com.google.protobuf.RpcController;

import edu.brown.hstore.HStoreCoordinator;
import edu.brown.hstore.HStoreSite;
import edu.brown.hstore.Hstoreservice.HStoreService;
import edu.brown.hstore.Hstoreservice.TransactionReduceRequest;
import edu.brown.hstore.Hstoreservice.TransactionReduceResponse;
import edu.brown.hstore.txns.LocalTransaction;
import edu.brown.hstore.txns.MapReduceTransaction;
import edu.brown.logging.LoggerUtil;
import edu.brown.logging.LoggerUtil.LoggerBoolean;
import edu.brown.protorpc.ProtoRpcController;
import edu.brown.utils.PartitionSet;

public class TransactionReduceHandler extends AbstractTransactionHandler<TransactionReduceRequest, TransactionReduceResponse> {
    private static final Logger LOG = Logger.getLogger(TransactionReduceHandler.class);
    private static final LoggerBoolean debug = new LoggerBoolean();
    private static final LoggerBoolean trace = new LoggerBoolean();
    static {
        LoggerUtil.attachObserver(LOG, debug, trace);
    }

    public TransactionReduceHandler(HStoreSite hstore_site,
            HStoreCoordinator hstore_coord) {
        super(hstore_site, hstore_coord);
    }

    @Override
    public void sendLocal(Long txn_id, TransactionReduceRequest request, PartitionSet partitions, RpcCallback<TransactionReduceResponse> callback) {
        // this should think about where to send it 
        // handler.transactionReduce(null, request, callback);
        this.remoteHandler(null, request, callback);
    }

    @Override
    public void sendRemote(HStoreService channel, ProtoRpcController controller, TransactionReduceRequest request,
            RpcCallback<TransactionReduceResponse> callback) {
        channel.transactionReduce(controller, request, callback);
    }

    @Override
    public void remoteQueue(RpcController controller, TransactionReduceRequest request,
            RpcCallback<TransactionReduceResponse> callback) {
        this.remoteHandler(controller, request, callback);
    }

    @Override
    public void remoteHandler(RpcController controller, TransactionReduceRequest request,
            RpcCallback<TransactionReduceResponse> callback) {
        assert (request.hasTransactionId()) : "Got Hstore." + request.getClass().getSimpleName() + " without a txn id!";
        long txn_id = request.getTransactionId();
        if (debug.val)
            LOG.debug(String.format("Got %s for txn #%d",
                      request.getClass().getSimpleName(), txn_id));
        
        MapReduceTransaction mr_ts = hstore_site.getTransaction(txn_id);
        assert(mr_ts != null);
        if (debug.val) 
            LOG.debug(String.format("TXN: %s, [Stage], [Site] %d",mr_ts,hstore_site.getSiteId())); 
       
        
        mr_ts.initTransactionReduceWrapperCallback(callback);
        
        if(!this.hstore_site.getLocalPartitionIds().contains(mr_ts.getBasePartition()))
            mr_ts.setReducePhase();
        
        assert(mr_ts.isReducePhase());
        
        if (debug.val)
            LOG.debug("After init initTransactionReduceWrapperCallback.......");
        
        /*
         * Here we would like to start MapReduce Transaction on the remote partition except the base partition of it.
         * This is to avoid the double invoke for remote task. 
         * */
        if(hstore_site.getHStoreConf().site.mr_reduce_blocking) {
            for (int partition : hstore_site.getLocalPartitionIds()) {
                if (partition != mr_ts.getBasePartition()) { 
                    LocalTransaction ts = mr_ts.getLocalTransaction(partition);
                    hstore_site.transactionStart(ts);
                }
            } // FOR
        } else {
            // non-blocking way of execution for Reduce
            mr_ts.markBasePartitionReduceExec();
            hstore_site.getMapReduceHelper().queue(mr_ts);
        }
        
    }

    @Override
    protected ProtoRpcController getProtoRpcController(LocalTransaction ts, int site_id) {
        return ts.getTransactionWorkController(site_id);
    }
}
