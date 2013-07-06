package edu.brown.hstore;

import java.nio.ByteBuffer;
import java.util.concurrent.BlockingQueue;

import org.apache.log4j.Logger;
import org.voltdb.ClientResponseImpl;
import org.voltdb.utils.Pair;

import com.google.protobuf.RpcCallback;

import edu.brown.hstore.util.AbstractProcessingRunnable;
import edu.brown.logging.LoggerUtil;
import edu.brown.logging.LoggerUtil.LoggerBoolean;

/**
 * Special thread that will process txn invocations
 * @author pavlo
 */
public class TransactionPreProcessor extends AbstractProcessingRunnable<Pair<ByteBuffer, RpcCallback<ClientResponseImpl>>> {
    private static final Logger LOG = Logger.getLogger(TransactionPreProcessor.class);
    private static final LoggerBoolean debug = new LoggerBoolean();
    private static final LoggerBoolean trace = new LoggerBoolean();
    static {
        LoggerUtil.attachObserver(LOG, debug, trace);
    }
    
    public TransactionPreProcessor(HStoreSite hstore_site,
                                   BlockingQueue<Pair<ByteBuffer, RpcCallback<ClientResponseImpl>>> queue) {
        super(hstore_site,
              HStoreConstants.THREAD_NAME_PREPROCESSOR,
              queue,
              hstore_site.getHStoreConf().site.status_exec_info);
    }
    
    @Override
    protected void processingCallback(Pair<ByteBuffer, RpcCallback<ClientResponseImpl>> next) {
        this.hstore_site.invocationProcess(next.getFirst(), next.getSecond());
    }
}
