package edu.brown.hstore;

import java.util.concurrent.BlockingQueue;

import org.apache.log4j.Logger;
import org.voltdb.ClientResponseImpl;

import com.google.protobuf.RpcCallback;

import edu.brown.hstore.util.AbstractProcessingRunnable;
import edu.brown.logging.LoggerUtil;
import edu.brown.logging.LoggerUtil.LoggerBoolean;

/**
 * Special thread that will process ClientResponses and send them back to clients 
 * @author pavlo
 */
public final class TransactionPostProcessor extends AbstractProcessingRunnable<Object[]> {
    private static final Logger LOG = Logger.getLogger(TransactionPostProcessor.class);
    private static final LoggerBoolean debug = new LoggerBoolean();
    static {
        LoggerUtil.attachObserver(LOG, debug);
    }

    /**
     * 
     * @param hstore_site
     */
    public TransactionPostProcessor(HStoreSite hstore_site,
                                    BlockingQueue<Object[]> queue) {
        super(hstore_site,
              HStoreConstants.THREAD_NAME_POSTPROCESSOR,
              queue,
              hstore_site.getHStoreConf().site.status_exec_info);
    }
    
    @Override
    protected void processingCallback(Object data[]) {
        ClientResponseImpl cresponse = (ClientResponseImpl)data[0];
        @SuppressWarnings("unchecked")
        RpcCallback<ClientResponseImpl> clientCallback = (RpcCallback<ClientResponseImpl>)data[1];
        long initiateTime = (Long)data[2];
        int restartCounter = (Integer)data[3];
        
        assert(cresponse != null);
        assert(clientCallback != null);
        
        if (debug.val)
            LOG.debug(String.format("Processing ClientResponse for txn #%d at partition %d [status=%s]",
                      cresponse.getTransactionId(), cresponse.getBasePartition(), cresponse.getStatus()));
        try {
            this.hstore_site.responseSend(cresponse, clientCallback, initiateTime, restartCounter);
        } catch (Throwable ex) {
            if (this.isShuttingDown() == false) throw new RuntimeException(ex);
            this.shutdown();
        }
    }
}
