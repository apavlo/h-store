package edu.brown.hstore.handlers;

import org.apache.log4j.Logger;
import org.voltdb.ParameterSet;
import org.voltdb.VoltTable;
import org.voltdb.exceptions.ServerFaultException;
import org.voltdb.messaging.FastDeserializer;

import com.google.protobuf.ByteString;
import com.google.protobuf.RpcCallback;
import com.google.protobuf.RpcController;

import edu.brown.hstore.HStoreCoordinator;
import edu.brown.hstore.HStoreSite;
import edu.brown.hstore.Hstoreservice.HStoreService;
import edu.brown.hstore.Hstoreservice.TransactionWorkRequest;
import edu.brown.hstore.Hstoreservice.TransactionWorkResponse;
import edu.brown.hstore.Hstoreservice.WorkFragment;
import edu.brown.hstore.callbacks.RemoteWorkCallback;
import edu.brown.hstore.txns.LocalTransaction;
import edu.brown.hstore.txns.RemoteTransaction;
import edu.brown.logging.LoggerUtil;
import edu.brown.logging.LoggerUtil.LoggerBoolean;
import edu.brown.protorpc.ProtoRpcController;
import edu.brown.utils.CollectionUtil;
import edu.brown.utils.PartitionSet;

public class TransactionWorkHandler extends AbstractTransactionHandler<TransactionWorkRequest, TransactionWorkResponse> {
    private static final Logger LOG = Logger.getLogger(TransactionWorkHandler.class);
    private static final LoggerBoolean debug = new LoggerBoolean();
    private static final LoggerBoolean trace = new LoggerBoolean();
    static {
        LoggerUtil.attachObserver(LOG, debug, trace);
    }
    
    public TransactionWorkHandler(HStoreSite hstore_site, HStoreCoordinator hstore_coord) {
        super(hstore_site, hstore_coord);
    }
    
    @Override
    public void sendLocal(Long txn_id, TransactionWorkRequest request, PartitionSet partitions, RpcCallback<TransactionWorkResponse> callback) {
        // TODO
    }
    @Override
    public void sendRemote(HStoreService channel, ProtoRpcController controller, TransactionWorkRequest request, RpcCallback<TransactionWorkResponse> callback) {
        channel.transactionWork(controller, request, callback);
    }
    @Override
    public void remoteQueue(RpcController controller, TransactionWorkRequest request, 
            RpcCallback<TransactionWorkResponse> callback) {
        if (debug.val)
            LOG.debug(String.format("Executing %s using remote handler for txn #%d",
                      request.getClass().getSimpleName(), request.getTransactionId()));
        this.remoteHandler(controller, request, callback);
    }
    @Override
    public void remoteHandler(RpcController controller, TransactionWorkRequest request,
            RpcCallback<TransactionWorkResponse> callback) {
        assert(request.hasTransactionId()) : "Got " + request.getClass().getSimpleName() + " without a txn id!";
        Long txn_id = Long.valueOf(request.getTransactionId());
        if (debug.val)
            LOG.debug(String.format("Got %s for txn #%d [partitionFragments=%d]",
                      request.getClass().getSimpleName(), txn_id, request.getFragmentsCount()));
        
        RemoteTransaction ts = hstore_site.getTransaction(txn_id);
        assert(ts != null);
        assert(txn_id.equals(ts.getTransactionId())) :
            String.format("Mismatched %s - Expected[%d] != Actual[%s]", ts, txn_id, ts.getTransactionId());
        
        // Deserialize embedded ParameterSets and store it in the RemoteTransaction handle
        // This way we only do it once per HStoreSite. This will also force us to avoid having
        // to do it for local work
        ParameterSet parameterSets[] = new ParameterSet[request.getParamsCount()]; // TODO: Cache!
        for (int i = 0; i < parameterSets.length; i++) {
            ByteString paramData = request.getParams(i);
            if (paramData != null && paramData.isEmpty() == false) {
                final FastDeserializer fds = new FastDeserializer(paramData.asReadOnlyByteBuffer());
                if (trace.val)
                    LOG.trace(String.format("Txn #%d paramData[%d] => %s",
                              txn_id, i, fds.buffer()));
                try {
                    parameterSets[i] = fds.readObject(ParameterSet.class);
                } catch (Exception ex) {
                    String msg = String.format("Failed to deserialize ParameterSet[%d] for txn #%d TransactionRequest", i, txn_id);
                    throw new ServerFaultException(msg, ex, txn_id);
                }
                // LOG.info("PARAMETER[" + i + "]: " + parameterSets[i]);
            } else {
                parameterSets[i] = ParameterSet.EMPTY;
            }
        } // FOR
        ts.attachParameterSets(parameterSets);
        
        // Deserialize attached VoltTable input dependencies
        FastDeserializer fds = null;
        VoltTable vt = null;
        for (int i = 0, cnt = request.getAttachedDataCount(); i < cnt; i++) {
            int input_dep_id = request.getAttachedDepId(i);
            ByteString data = request.getAttachedData(i);
            if (data.isEmpty()) {
                String msg = String.format("%s input dependency %d is empty", ts, input_dep_id); 
                LOG.warn(msg + "\n" + request);
                throw new ServerFaultException(msg, txn_id);
            }
            
            if (fds == null) fds = new FastDeserializer(data.asReadOnlyByteBuffer());
            else fds.setBuffer(data.asReadOnlyByteBuffer());

            vt = null;
            try {
                vt = fds.readObject(VoltTable.class);
            } catch (Exception ex) {
                String msg = String.format("Failed to deserialize VoltTable[%d] for txn #%d", input_dep_id, txn_id); 
                throw new ServerFaultException(msg, ex, txn_id);
            }
            assert(vt != null);
            ts.attachInputDependency(input_dep_id, vt);
        } // FOR
        
        // This is work from a transaction executing at another node
        // Any other message can just be sent along to the ExecutionSite without sending
        // back anything right away. The ExecutionSite will use our wrapped callback handle
        // to send back whatever response it needs to, but we won't actually send it
        // until we get back results from all of the partitions
        // TODO: The base information of a set of FragmentTaskMessages should be moved into
        // the message wrapper (e.g., base partition, client handle)
        boolean first = true;
        int fragmentCount = request.getFragmentsCount(); 
        PartitionSet partitions = null;
        if (fragmentCount == 1) {
            WorkFragment work = CollectionUtil.first(request.getFragmentsList());
            partitions = this.catalogContext.getPartitionSetSingleton(work.getPartitionId());
        }
        else {
            partitions = new PartitionSet();
            for (WorkFragment work : request.getFragmentsList()) {
                partitions.add(work.getPartitionId());
            } // FOR
        }
        
        for (WorkFragment work : request.getFragmentsList()) {
            // Always initialize the TransactionWorkCallback for the first callback 
            if (first) {
                RemoteWorkCallback work_callback = ts.getWorkCallback();
                if (work_callback.isInitialized()) work_callback.finish(); // HACK
                work_callback.init(ts, partitions, callback);
                if (debug.val)
                    LOG.debug(String.format("Initializing %s for %s",
                              work_callback.getClass().getSimpleName(), ts));
            }
            
            if (debug.val)
                LOG.debug(String.format("Invoking transactionWork for %s [first=%s]", ts, first));
            hstore_site.transactionWork(ts, work);
            first = false;
        } // FOR
        
        // We don't need to send back a response right here.
        // TransactionWorkCallback will wait until it has results from all of the partitions 
        // the tasks were sent to and then send back everything in a single response message
        
    }
    @Override
    protected ProtoRpcController getProtoRpcController(LocalTransaction ts, int site_id) {
        return ts.getTransactionWorkController(site_id);
    }

}
