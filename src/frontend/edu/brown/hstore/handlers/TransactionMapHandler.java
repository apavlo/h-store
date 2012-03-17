package edu.brown.hstore.handlers;

import java.util.Collection;

import org.apache.log4j.Logger;
import org.voltdb.StoredProcedureInvocation;
import org.voltdb.messaging.FastDeserializer;

import com.google.protobuf.RpcCallback;
import com.google.protobuf.RpcController;

import edu.brown.hstore.HStoreCoordinator;
import edu.brown.hstore.HStoreSite;
import edu.brown.hstore.Hstoreservice.HStoreService;
import edu.brown.hstore.Hstoreservice.TransactionMapRequest;
import edu.brown.hstore.Hstoreservice.TransactionMapResponse;
import edu.brown.hstore.dtxn.LocalTransaction;
import edu.brown.hstore.dtxn.MapReduceTransaction;
import edu.brown.logging.LoggerUtil;
import edu.brown.logging.LoggerUtil.LoggerBoolean;
import edu.brown.protorpc.ProtoRpcController;

public class TransactionMapHandler extends AbstractTransactionHandler<TransactionMapRequest, TransactionMapResponse> {
    private static final Logger LOG = Logger.getLogger(TransactionMapHandler.class);
    private static final LoggerBoolean debug = new LoggerBoolean(LOG.isDebugEnabled());
    private static final LoggerBoolean trace = new LoggerBoolean(LOG.isTraceEnabled());
    static {
        LoggerUtil.attachObserver(LOG, debug, trace);
    }
    
    //final Dispatcher<Object[]> MapDispatcher;
    
    public TransactionMapHandler(HStoreSite hstore_site, HStoreCoordinator hstore_coord) {
        super(hstore_site, hstore_coord);
    }
    
    @Override
    public void sendLocal(Long txn_id, TransactionMapRequest request, Collection<Integer> partitions, RpcCallback<TransactionMapResponse> callback) {
        // This is for MapReduce Transaction, the local task is still passed to the remoteHandler to be invoked the TransactionStart
        // as the a LocalTransaction. This LocalTransaction in this partition is the base partition for MR transaction.
        if (debug.get()) LOG.debug("Send to remoteHandler from sendLocal");
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
    public void remoteHandler(RpcController controller, TransactionMapRequest request,
            RpcCallback<TransactionMapResponse> callback) {
        assert(request.hasTransactionId()) : "Got Hstore." + request.getClass().getSimpleName() + " without a txn id!";
        Long txn_id = Long.valueOf(request.getTransactionId());
        if (debug.get())
            LOG.debug("__FILE__:__LINE__ " + String.format("Got %s for txn #%d",
                                   request.getClass().getSimpleName(), txn_id));

        // Deserialize the StoredProcedureInvocation object
        StoredProcedureInvocation invocation = null;
        try {
            invocation = FastDeserializer.deserialize(request.getInvocation().asReadOnlyByteBuffer(), StoredProcedureInvocation.class);
        } catch (Exception ex) {
            throw new RuntimeException("Unexpected error when deserializing StoredProcedureInvocation", ex);
        }
        // build parameterSet is important here
        // This is the new version that should be build here 
        invocation.buildParameterSet();
        if (debug.get())
            LOG.debug("__FILE__:__LINE__ " + String.format("Check invocation: %s, Procedure Name: %s , parameters:%s, Base Partition:%d",
                    invocation,invocation.getProcName(), invocation.getParams(), request.getBasePartition()));
        assert(invocation != null);
        assert(invocation.getParams() != null);
        
        MapReduceTransaction mr_ts = hstore_site.getTransaction(txn_id);
        
        // The mr_ts handle will be null if this HStoreSite is not where the 
        // base partition for the original MRTransaction
        if (mr_ts == null) {
            assert(invocation != null):"invocation== null\n";
            mr_ts = hstore_site.createMapReduceTransaction(txn_id, invocation, request.getBasePartition());
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
                hstore_site.transactionStart(ts, partition);
            }
        } // FOR
    }
    @Override
    protected ProtoRpcController getProtoRpcController(LocalTransaction ts, int site_id) {
        return ts.getTransactionWorkController(site_id);
    }
}
