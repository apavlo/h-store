package edu.mit.hstore.handlers;

import java.util.Collection;

import org.apache.log4j.Logger;
import org.voltdb.messaging.FragmentTaskMessage;
import org.voltdb.messaging.VoltMessage;

import ca.evanjones.protorpc.ProtoRpcController;

import com.google.protobuf.RpcCallback;
import com.google.protobuf.RpcController;

import edu.brown.hstore.Hstore.HStoreService;
import edu.brown.hstore.Hstore.TransactionWorkRequest;
import edu.brown.hstore.Hstore.TransactionWorkResponse;
import edu.brown.hstore.Hstore.TransactionWorkRequest.PartitionFragment;
import edu.brown.logging.LoggerUtil;
import edu.brown.logging.LoggerUtil.LoggerBoolean;
import edu.mit.hstore.HStoreCoordinator;
import edu.mit.hstore.HStoreSite;
import edu.mit.hstore.callbacks.TransactionWorkCallback;
import edu.mit.hstore.dtxn.LocalTransaction;
import edu.mit.hstore.dtxn.RemoteTransaction;

public class TransactionWorkHandler extends AbstractTransactionHandler<TransactionWorkRequest, TransactionWorkResponse> {
    private static final Logger LOG = Logger.getLogger(TransactionWorkHandler.class);
    private static final LoggerBoolean debug = new LoggerBoolean(LOG.isDebugEnabled());
    private static final LoggerBoolean trace = new LoggerBoolean(LOG.isTraceEnabled());
    static {
        LoggerUtil.attachObserver(LOG, debug, trace);
    }
    
    public TransactionWorkHandler(HStoreSite hstore_site, HStoreCoordinator hstore_coord) {
        super(hstore_site, hstore_coord);
    }
    
    @Override
    public void sendLocal(long txn_id, TransactionWorkRequest request, Collection<Integer> partitions, RpcCallback<TransactionWorkResponse> callback) {
        // TODO
    }
    @Override
    public void sendRemote(HStoreService channel, ProtoRpcController controller, TransactionWorkRequest request, RpcCallback<TransactionWorkResponse> callback) {
        channel.transactionWork(controller, request, callback);
    }
    @Override
    public void remoteQueue(RpcController controller, TransactionWorkRequest request, 
            RpcCallback<TransactionWorkResponse> callback) {
        if (debug.get())
            LOG.debug("__FILE__:__LINE__ " + String.format("Executing %s using remote handler for txn #%d",
                      request.getClass().getSimpleName(), request.getTransactionId()));
        this.remoteHandler(controller, request, callback);
    }
    @Override
    public void remoteHandler(RpcController controller, TransactionWorkRequest request,
            RpcCallback<TransactionWorkResponse> callback) {
        assert(request.hasTransactionId()) : "Got Hstore." + request.getClass().getSimpleName() + " without a txn id!";
        long txn_id = request.getTransactionId();
        if (debug.get())
            LOG.debug("__FILE__:__LINE__ " + String.format("Got %s for txn #%d [partitionFragments=%d]",
                                   request.getClass().getSimpleName(), txn_id, request.getFragmentsCount()));
        
        // This is work from a transaction executing at another node
        // Any other message can just be sent along to the ExecutionSite without sending
        // back anything right away. The ExecutionSite will use our wrapped callback handle
        // to send back whatever response it needs to, but we won't actually send it
        // until we get back results from all of the partitions
        // TODO: The base information of a set of FragmentTaskMessages should be moved into
        // the message wrapper (e.g., base partition, client handle)
        RemoteTransaction ts = hstore_site.getTransaction(txn_id);
        FragmentTaskMessage ftask = null;
        boolean first = true;
        for (PartitionFragment partition_task : request.getFragmentsList()) {
            // Decode the inner VoltMessage
            try {
                ftask = (FragmentTaskMessage)VoltMessage.createMessageFromBuffer(partition_task.getWork().asReadOnlyByteBuffer(), false);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
            assert(txn_id == ftask.getTxnId()) :
                String.format("%s is for txn #%d but the %s has txn #%d",
                              request.getClass().getSimpleName(), txn_id,
                              ftask.getClass().getSimpleName(), ftask.getTxnId());

            // If this is the first time we've been here, then we need to create a RemoteTransaction handle
            if (ts == null) {
                ts = hstore_site.createRemoteTransaction(txn_id, ftask);
            }
            
            // Always initialize the TransactionWorkCallback for the first callback 
            if (first) {
                TransactionWorkCallback work_callback = ts.getFragmentTaskCallback();
                if (work_callback.isInitialized()) work_callback.finish();
                work_callback.init(txn_id, request.getFragmentsCount(), callback);
            }
            
            hstore_site.transactionWork(ts, ftask);
            first = false;
        } // FOR
        assert(ts != null);
        assert(txn_id == ts.getTransactionId()) :
            String.format("Mismatched %s - Expected[%d] != Actual[%s]", ts, txn_id, ts.getTransactionId());
        
        // We don't need to send back a response right here.
        // TransactionWorkCallback will wait until it has results from all of the partitions 
        // the tasks were sent to and then send back everything in a single response message
        
    }
    @Override
    protected ProtoRpcController getProtoRpcController(LocalTransaction ts, int site_id) {
        return ts.getTransactionWorkController(site_id);
    }

}
