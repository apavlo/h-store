package edu.mit.hstore.handlers;

import java.nio.ByteBuffer;
import java.util.Collection;

import org.apache.log4j.Logger;
import org.voltdb.VoltTable;
import org.voltdb.messaging.FastDeserializer;

import ca.evanjones.protorpc.ProtoRpcController;

import com.google.protobuf.ByteString;
import com.google.protobuf.RpcCallback;
import com.google.protobuf.RpcController;

import edu.brown.hstore.Hstore;
import edu.brown.hstore.Hstore.HStoreService;
import edu.brown.hstore.Hstore.SendDataRequest;
import edu.brown.hstore.Hstore.SendDataResponse;
import edu.brown.logging.LoggerUtil;
import edu.brown.logging.LoggerUtil.LoggerBoolean;
import edu.brown.utils.StringUtil;
import edu.mit.dtxn.Dtxn.FragmentResponse.Status;
import edu.mit.hstore.HStoreCoordinator;
import edu.mit.hstore.HStoreSite;
import edu.mit.hstore.dtxn.AbstractTransaction;
import edu.mit.hstore.dtxn.LocalTransaction;

public class SendDataHandler extends AbstractTransactionHandler<SendDataRequest, SendDataResponse> {
    private static final Logger LOG = Logger.getLogger(SendDataHandler.class);
    private static final LoggerBoolean debug = new LoggerBoolean(LOG.isDebugEnabled());
    private static final LoggerBoolean trace = new LoggerBoolean(LOG.isTraceEnabled());
    static {
        LoggerUtil.attachObserver(LOG, debug, trace);
    }
    
    //final Dispatcher<Object[]> MapDispatcher;
    
    public SendDataHandler(HStoreSite hstore_site, HStoreCoordinator hstore_coord) {
        super(hstore_site, hstore_coord);
    }
    
    @Override
    public void sendLocal(long txn_id, SendDataRequest request, Collection<Integer> partitions, RpcCallback<SendDataResponse> callback) {
        // We should never be called because we never want to have serialize/deserialize data
        // within our own process
        assert(false): this.getClass().getSimpleName() + ".sendLocal should never be called!";
    }
    @Override
    public void sendRemote(HStoreService channel, ProtoRpcController controller, SendDataRequest request, RpcCallback<SendDataResponse> callback) {
        channel.sendData(controller, request, callback);
    }
    @Override
    public void remoteQueue(RpcController controller, SendDataRequest request,
            RpcCallback<SendDataResponse> callback) {
        this.remoteHandler(controller, request, callback);
    }
    @Override
    public void remoteHandler(RpcController controller, SendDataRequest request,
            RpcCallback<SendDataResponse> callback) {
        assert(request.hasTransactionId()) : "Got Hstore." + request.getClass().getSimpleName() + " without a txn id!";
        long txn_id = request.getTransactionId();
        
        if (debug.get())
            LOG.debug("__FILE__:__LINE__ " + String.format("Got %s for txn #%d",
                                   request.getClass().getSimpleName(), txn_id));

        AbstractTransaction ts = hstore_site.getTransaction(txn_id);
        assert(ts != null) : "Unexpected transaction #" + txn_id;

        Hstore.SendDataResponse.Builder builder = Hstore.SendDataResponse.newBuilder()
                                                             .setTransactionId(txn_id)
                                                             .setStatus(Hstore.Status.OK)
                                                             .setSenderId(hstore_site.getSiteId());
        
        for (Hstore.PartitionFragment frag : request.getFragmentsList()) {
            int partition = frag.getPartitionId();
            
            assert(hstore_site.getLocalPartitionIds().contains(partition));
            ByteBuffer data = frag.getData().asReadOnlyByteBuffer();
            assert(data != null);
            
            // Deserialize the VoltTable object for the given byte array
            VoltTable vt = null;
            try {
                vt = FastDeserializer.deserialize(data, VoltTable.class);
                
            } catch (Exception ex) {
                LOG.warn("Unexpected error when deserializing VoltTable", ex);
            }
            assert(vt != null);
            if (debug.get()) {
                byte bytes[] = frag.getData().toByteArray();
                LOG.debug(String.format("Inbound data for Partition #%d: RowCount=%d / MD5=%s / Length=%d",
                                        partition, vt.getRowCount(),StringUtil.md5sum(bytes), bytes.length));
            }
            
            if (debug.get())
                LOG.debug(String.format("<StoreTable from Partition %d to Partition:%d>\n %s",hstore_site.getSiteId() ,partition,vt));
            Hstore.Status status = ts.storeData(partition, vt);
            if (status != Hstore.Status.OK) builder.setStatus(status);
            builder.addPartitions(partition);
        } // FOR
        
        callback.run(builder.build());
    }
    @Override
    protected ProtoRpcController getProtoRpcController(LocalTransaction ts, int site_id) {
        return ts.getTransactionWorkController(site_id);
    }
}
