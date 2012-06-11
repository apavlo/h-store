package edu.brown.hstore;

import java.nio.ByteBuffer;
import java.util.concurrent.BlockingQueue;

import org.apache.log4j.Logger;
import org.voltdb.ClientResponseImpl;
import org.voltdb.utils.Pair;

import com.google.protobuf.RpcCallback;

import edu.brown.hstore.interfaces.Shutdownable;
import edu.brown.logging.LoggerUtil;
import edu.brown.logging.LoggerUtil.LoggerBoolean;

public class TransactionPreProcessor implements Runnable, Shutdownable {
    private static final Logger LOG = Logger.getLogger(TransactionPreProcessor.class);
    private static final LoggerBoolean debug = new LoggerBoolean(LOG.isDebugEnabled());
    private static final LoggerBoolean trace = new LoggerBoolean(LOG.isTraceEnabled());
    static {
        LoggerUtil.attachObserver(LOG, debug, trace);
    }
    
    private final HStoreSite hstore_site;
    private final BlockingQueue<Pair<ByteBuffer, RpcCallback<ClientResponseImpl>>> queue;
    private boolean stop = false;
    private Thread self;
    
    public TransactionPreProcessor(HStoreSite hstore_site,
                                    BlockingQueue<Pair<ByteBuffer, RpcCallback<ClientResponseImpl>>> queue) {
        assert(queue != null);
        this.hstore_site = hstore_site;
        this.queue = queue;
    }
    
    @Override
    public void run() {
        this.self = Thread.currentThread();
        this.self.setName(HStoreThreadManager.getThreadName(hstore_site, HStoreConstants.THREAD_NAME_PREPROCESSOR));
        hstore_site.getThreadManager().registerProcessingThread();

        Pair<ByteBuffer, RpcCallback<ClientResponseImpl>> p = null;
        while (this.stop == false) {
            try {
                p = this.queue.take();
            } catch (InterruptedException ex) {
                this.stop = true;
                break;
            }
            this.hstore_site.processInvocation(p.getFirst(), p.getSecond());
        } // WHILE
    }


    @Override
    public void prepareShutdown(boolean error) {
        this.stop = true;
    }


    @Override
    public void shutdown() {
        if (this.self != null) this.self.interrupt();
    }


    @Override
    public boolean isShuttingDown() {
        return (this.stop);
    }
}
