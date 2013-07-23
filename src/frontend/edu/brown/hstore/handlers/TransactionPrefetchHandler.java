package edu.brown.hstore.handlers;

import java.io.IOException;

import org.apache.log4j.Logger;
import org.voltdb.VoltTable;
import org.voltdb.exceptions.ServerFaultException;
import org.voltdb.messaging.FastDeserializer;

import com.google.protobuf.RpcCallback;
import com.google.protobuf.RpcController;

import edu.brown.hstore.HStoreCoordinator;
import edu.brown.hstore.HStoreSite;
import edu.brown.hstore.Hstoreservice.HStoreService;
import edu.brown.hstore.Hstoreservice.Status;
import edu.brown.hstore.Hstoreservice.TransactionPrefetchAcknowledgement;
import edu.brown.hstore.Hstoreservice.TransactionPrefetchResult;
import edu.brown.hstore.Hstoreservice.WorkResult;
import edu.brown.hstore.PartitionExecutor;
import edu.brown.hstore.txns.LocalTransaction;
import edu.brown.logging.LoggerUtil;
import edu.brown.logging.LoggerUtil.LoggerBoolean;
import edu.brown.protorpc.ProtoRpcController;
import edu.brown.utils.PartitionSet;

/**
 * Process TransactionPrefetchResult sent from remote sites for prefetched queries
 * @author pavlo
 */
public class TransactionPrefetchHandler extends AbstractTransactionHandler<TransactionPrefetchResult, TransactionPrefetchAcknowledgement> {
    private static final Logger LOG = Logger.getLogger(TransactionWorkHandler.class);
    private static final LoggerBoolean debug = new LoggerBoolean();
    private static final LoggerBoolean trace = new LoggerBoolean();
    static {
        LoggerUtil.attachObserver(LOG, debug, trace);
    }
    
    private final FastDeserializer fds = new FastDeserializer();
    
    public TransactionPrefetchHandler(HStoreSite hstore_site, HStoreCoordinator hstore_coord) {
        super(hstore_site, hstore_coord);
    }
    
    @Override
    public void sendLocal(Long txn_id, TransactionPrefetchResult request, PartitionSet partitions, RpcCallback<TransactionPrefetchAcknowledgement> callback) {
        // TODO
    }
    @Override
    public void sendRemote(HStoreService channel, ProtoRpcController controller, TransactionPrefetchResult request, RpcCallback<TransactionPrefetchAcknowledgement> callback) {
        channel.transactionPrefetch(controller, request, callback);
    }
    @Override
    public void remoteQueue(RpcController controller, TransactionPrefetchResult request, RpcCallback<TransactionPrefetchAcknowledgement> callback) {
        if (debug.val)
            LOG.debug(String.format("Executing %s using remote handler for txn #%d",
                      request.getClass().getSimpleName(), request.getTransactionId()));
        this.remoteHandler(controller, request, callback);
    }
    @Override
    public void remoteHandler(RpcController controller, TransactionPrefetchResult request, RpcCallback<TransactionPrefetchAcknowledgement> callback) {
        assert(request.hasTransactionId()) : 
            "Got " + request.getClass().getSimpleName() + " without a txn id!";
        Long txn_id = Long.valueOf(request.getTransactionId());
        if (debug.val)
            LOG.debug(String.format("Got %s for txn #%d [remotePartition=%d]",
                      request.getClass().getSimpleName(), txn_id, request.getSourcePartition()));
        
        // We should never a get a TransactionPrefetchResult for a transaction that
        // we don't know about.
        LocalTransaction ts = hstore_site.getTransaction(txn_id);
        if (ts == null) {
            String msg = String.format("Unexpected transaction id %d for incoming %s",
                                       txn_id, request.getClass().getSimpleName());
            throw new ServerFaultException(msg, txn_id);
        }
        
        // We want to store this before sending back the acknowledgment so that the transaction can get
        // access to it right away
        PartitionExecutor executor = hstore_site.getPartitionExecutor(ts.getBasePartition());
        WorkResult result = request.getResult();
        
        if (result.getStatus() != Status.OK) {
            // TODO: Process error!
        } else {
            for (int i = 0, cnt = result.getDepIdCount(); i < cnt; i++) {
                int fragmentId = request.getFragmentId(i);
                int stmtCounter = request.getStmtCounter(i);
                int paramsHash = request.getParamHash(i);
                
                VoltTable vt = null;
                try {
                    this.fds.setBuffer(result.getDepData(i).asReadOnlyByteBuffer());
                    vt = this.fds.readObject(VoltTable.class);
                } catch (IOException ex) {
                    throw new RuntimeException(ex);
                }
        
                executor.addPrefetchResult(ts, stmtCounter, fragmentId,
                                           request.getSourcePartition(),
                                           paramsHash, vt);
            } // FOR
        }
        
        
        // I don't think we even need to bother wasting our time sending an acknowledgement
        // We would like to cancel but we can't do that on the "server" side
        TransactionPrefetchAcknowledgement response = TransactionPrefetchAcknowledgement.newBuilder()
                                                            .setTransactionId(txn_id.longValue())
                                                            .setTargetPartition(request.getSourcePartition())
                                                            .build();
        callback.run(response);
    }
    @Override
    protected ProtoRpcController getProtoRpcController(LocalTransaction ts, int site_id) {
        return ts.getTransactionWorkController(site_id);
    }

}
