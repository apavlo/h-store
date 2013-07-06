package edu.brown.hstore.handlers;

import java.nio.ByteBuffer;

import org.apache.log4j.Logger;
import org.voltdb.VoltTable;
import org.voltdb.messaging.FastDeserializer;

import com.google.protobuf.RpcCallback;
import com.google.protobuf.RpcController;

import edu.brown.hstore.HStoreCoordinator;
import edu.brown.hstore.HStoreSite;
import edu.brown.hstore.Hstoreservice;
import edu.brown.hstore.Hstoreservice.HStoreService;
import edu.brown.hstore.Hstoreservice.SendDataRequest;
import edu.brown.hstore.Hstoreservice.SendDataResponse;
import edu.brown.hstore.txns.AbstractTransaction;
import edu.brown.hstore.txns.LocalTransaction;
import edu.brown.logging.LoggerUtil;
import edu.brown.logging.LoggerUtil.LoggerBoolean;
import edu.brown.protorpc.ProtoRpcController;
import edu.brown.utils.PartitionSet;
import edu.brown.utils.StringUtil;

public class SendDataHandler extends AbstractTransactionHandler<SendDataRequest, SendDataResponse> {
    private static final Logger LOG = Logger.getLogger(SendDataHandler.class);
    private static final LoggerBoolean debug = new LoggerBoolean();
    private static final LoggerBoolean trace = new LoggerBoolean();
    static {
        LoggerUtil.attachObserver(LOG, debug, trace);
    }
    
    //final Dispatcher<Object[]> MapDispatcher;
    
    public SendDataHandler(HStoreSite hstore_site, HStoreCoordinator hstore_coord) {
        super(hstore_site, hstore_coord);
    }
    
    @Override
    public void sendLocal(Long txn_id, SendDataRequest request, PartitionSet partitions, RpcCallback<SendDataResponse> callback) {
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
        
        if (debug.val)
            LOG.debug("__FILE__:__LINE__ " + String.format("Got %s for txn #%d",
                                   request.getClass().getSimpleName(), txn_id));

        AbstractTransaction ts = hstore_site.getTransaction(txn_id);
        assert(ts != null) : "Unexpected transaction #" + txn_id;

        SendDataResponse.Builder builder = SendDataResponse.newBuilder()
                                                             .setTransactionId(txn_id)
                                                             .setStatus(Hstoreservice.Status.OK)
                                                             .setSenderSite(hstore_site.getSiteId());
        
        for (int i = 0, cnt = request.getDataCount(); i < cnt; i++) {
            int partition = request.getDepId(i);
            assert(hstore_site.getLocalPartitionIds().contains(partition));
            
            ByteBuffer data = request.getData(i).asReadOnlyByteBuffer();
            assert(data != null);
                
            // Deserialize the VoltTable object for the given byte array
            VoltTable vt = null;
            try {
                vt = FastDeserializer.deserialize(data, VoltTable.class);
            } catch (Exception ex) {
                LOG.warn("Unexpected error when deserializing VoltTable", ex);
            }
            assert(vt != null);
            if (debug.val) {
                byte bytes[] = request.getData(i).toByteArray();
                LOG.debug(String.format("Inbound data for Partition #%d: RowCount=%d / MD5=%s / Length=%d",
                                        partition, vt.getRowCount(),StringUtil.md5sum(bytes), bytes.length));
            }
                
            if (debug.val)
                LOG.debug(String.format("<StoreTable from Partition %d to Partition:%d>\n %s",hstore_site.getSiteId() ,partition,vt));
            Hstoreservice.Status status = ts.storeData(partition, vt);
            if (status != Hstoreservice.Status.OK) builder.setStatus(status);
            builder.addPartitions(partition);
        } // FOR
        
        callback.run(builder.build());
    }
    @Override
    protected ProtoRpcController getProtoRpcController(LocalTransaction ts, int site_id) {
        return ts.getTransactionWorkController(site_id);
    }
}
