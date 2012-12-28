package edu.brown.hstore;

import java.util.concurrent.BlockingQueue;

import org.apache.log4j.Logger;
import org.voltdb.ClientResponseImpl;
import org.voltdb.utils.Pair;

import edu.brown.hstore.txns.LocalTransaction;
import edu.brown.hstore.util.AbstractProcessingThread;
import edu.brown.logging.LoggerUtil;
import edu.brown.logging.LoggerUtil.LoggerBoolean;

/**
 * Special thread that will process ClientResponses and send them back to clients 
 * @author pavlo
 */
public final class TransactionPostProcessor extends AbstractProcessingThread<Pair<LocalTransaction, ClientResponseImpl>> {
    private static final Logger LOG = Logger.getLogger(TransactionPostProcessor.class);
    private static final LoggerBoolean debug = new LoggerBoolean(LOG.isDebugEnabled());
    private static final LoggerBoolean trace = new LoggerBoolean(LOG.isTraceEnabled());
    static {
        LoggerUtil.attachObserver(LOG, debug, trace);
    }

    /**
     * 
     * @param hstore_site
     */
    public TransactionPostProcessor(HStoreSite hstore_site,
                                    BlockingQueue<Pair<LocalTransaction, ClientResponseImpl>> queue) {
        super(hstore_site,
              HStoreConstants.THREAD_NAME_POSTPROCESSOR,
              queue,
              hstore_site.getHStoreConf().site.status_show_executor_info);
    }
    
    @Override
    protected void processingCallback(Pair<LocalTransaction, ClientResponseImpl> pair) {
        LocalTransaction ts = pair.getFirst();
        assert(ts != null);
        ClientResponseImpl cr = pair.getSecond();
        assert(cr != null);
        
        if (debug.val) LOG.debug(String.format("Processing ClientResponse for %s at partition %d [status=%s]",
                                                 ts, ts.getBasePartition(), cr.getStatus()));
        try {
            hstore_site.responseSend(ts, cr);
            hstore_site.queueDeleteTransaction(ts.getTransactionId(), cr.getStatus());
        } catch (Throwable ex) {
            LOG.error(String.format("Failed to process %s properly\n%s", ts, cr));
            if (this.isShuttingDown() == false) throw new RuntimeException(ex);
            this.shutdown();
        }
    }
}
