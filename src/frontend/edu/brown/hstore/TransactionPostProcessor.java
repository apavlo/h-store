package edu.brown.hstore;

import java.util.concurrent.BlockingQueue;

import org.apache.log4j.Logger;
import org.voltdb.ClientResponseImpl;

import com.google.protobuf.RpcCallback;

import edu.brown.hstore.txns.LocalTransaction;
import edu.brown.hstore.util.AbstractProcessingRunnable;
import edu.brown.logging.LoggerUtil;
import edu.brown.logging.LoggerUtil.LoggerBoolean;

/**
 * Special thread that will process ClientResponses and send them back to clients 
 * @author pavlo
 */
public final class TransactionPostProcessor extends AbstractProcessingRunnable<LocalTransaction> {
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
                                    BlockingQueue<LocalTransaction> queue) {
        super(hstore_site,
              HStoreConstants.THREAD_NAME_POSTPROCESSOR,
              queue,
              hstore_site.getHStoreConf().site.status_exec_info);
    }
    
    @Override
    protected void processingCallback(LocalTransaction ts) {
        ClientResponseImpl cresponse = ts.getClientResponse();
        RpcCallback<ClientResponseImpl> clientCallback = ts.getClientCallback();
        long initiateTime = ts.getInitiateTime();
        int restartCounter = ts.getRestartCounter();
        
        assert(cresponse != null);
        assert(clientCallback != null);
        
        if (debug.val)
            LOG.debug(String.format("Processing ClientResponse for %s at partition %d [status=%s]",
                      ts, ts.getBasePartition(), cresponse.getStatus()));
        try {
            this.hstore_site.responseSend(cresponse, clientCallback, initiateTime, restartCounter);
        } catch (Throwable ex) {
            if (this.isShuttingDown() == false) throw new RuntimeException(ex);
            this.shutdown();
        }
    }
}
