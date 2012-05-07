package edu.brown.hstore.handlers;

import java.util.Collection;

import org.apache.log4j.Logger;
import org.voltdb.exceptions.ServerFaultException;

import com.google.protobuf.RpcCallback;
import com.google.protobuf.RpcController;

import edu.brown.hstore.HStoreCoordinator;
import edu.brown.hstore.HStoreSite;
import edu.brown.hstore.Hstoreservice.HStoreService;
import edu.brown.hstore.Hstoreservice.TransactionPrefetchAcknowledgement;
import edu.brown.hstore.Hstoreservice.TransactionPrefetchResult;
import edu.brown.hstore.dtxn.LocalTransaction;
import edu.brown.logging.LoggerUtil;
import edu.brown.logging.LoggerUtil.LoggerBoolean;
import edu.brown.protorpc.ProtoRpcController;

public class TransactionPrefetchHandler extends AbstractTransactionHandler<TransactionPrefetchResult, TransactionPrefetchAcknowledgement> {
    private static final Logger LOG = Logger.getLogger(TransactionWorkHandler.class);
    private static final LoggerBoolean debug = new LoggerBoolean(LOG.isDebugEnabled());
    private static final LoggerBoolean trace = new LoggerBoolean(LOG.isTraceEnabled());
    static {
        LoggerUtil.attachObserver(LOG, debug, trace);
    }
    
    public TransactionPrefetchHandler(HStoreSite hstore_site, HStoreCoordinator hstore_coord) {
        super(hstore_site, hstore_coord);
    }
    
    @Override
    public void sendLocal(Long txn_id, TransactionPrefetchResult request, Collection<Integer> partitions, RpcCallback<TransactionPrefetchAcknowledgement> callback) {
        // TODO
    }
    @Override
    public void sendRemote(HStoreService channel, ProtoRpcController controller, TransactionPrefetchResult request, RpcCallback<TransactionPrefetchAcknowledgement> callback) {
        channel.transactionPrefetch(controller, request, callback);
    }
    @Override
    public void remoteQueue(RpcController controller, TransactionPrefetchResult request, RpcCallback<TransactionPrefetchAcknowledgement> callback) {
        if (debug.get())
            LOG.debug(String.format("Executing %s using remote handler for txn #%d",
                      request.getClass().getSimpleName(), request.getTransactionId()));
        this.remoteHandler(controller, request, callback);
    }
    @Override
    public void remoteHandler(RpcController controller, TransactionPrefetchResult request, RpcCallback<TransactionPrefetchAcknowledgement> callback) {
        assert(request.hasTransactionId()) : 
            "Got " + request.getClass().getSimpleName() + " without a txn id!";
        Long txn_id = Long.valueOf(request.getTransactionId());
        if (debug.get()) LOG.debug(String.format("Got %s for txn #%d [remotePartition=%d]",
                                                 request.getClass().getSimpleName(), txn_id, request.getSourcePartition()));
        
        // We should never a get a TransactionPrefetchResult for a transaction that
        // we don't know about.
        // XXX: No I think it's ok because we 
        LocalTransaction ts = hstore_site.getTransaction(txn_id);
        if (ts == null) {
            String msg = String.format("Unexpected transaction id %d for incoming %s",
                                       txn_id, request.getClass().getSimpleName());
            throw new ServerFaultException(msg, txn_id);
        }
        
        // We want to store this before sending back the acknowledgment so that the transaction can get
        // access to it right away
        ts.addPrefetchResults(request.getResult());
        
        // I don't think we even need to bother wasting our time sending an acknowledgement
//        TransactionPrefetchAcknowledgement response = TransactionPrefetchAcknowledgement.newBuilder()
//                                                            .setTransactionId(txn_id.longValue())
//                                                            .setTargetPartition(request.getSourcePartition())
//                                                            .build();
//        callback.run(response);
    }
    @Override
    protected ProtoRpcController getProtoRpcController(LocalTransaction ts, int site_id) {
        return ts.getTransactionWorkController(site_id);
    }

}
