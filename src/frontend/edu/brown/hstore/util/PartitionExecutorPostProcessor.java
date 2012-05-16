package edu.brown.hstore.util;

import java.util.concurrent.LinkedBlockingDeque;

import org.apache.log4j.Logger;
import org.voltdb.ClientResponseImpl;
import org.voltdb.utils.Pair;

import edu.brown.hstore.HStoreSite;
import edu.brown.hstore.HStoreThreadManager;
import edu.brown.hstore.conf.HStoreConf;
import edu.brown.hstore.dtxn.LocalTransaction;
import edu.brown.hstore.interfaces.Shutdownable;
import edu.brown.logging.LoggerUtil;
import edu.brown.logging.LoggerUtil.LoggerBoolean;
import edu.brown.utils.ProfileMeasurement;

public final class PartitionExecutorPostProcessor implements Runnable, Shutdownable {
    private static final Logger LOG = Logger.getLogger(PartitionExecutorPostProcessor.class);
    private static final LoggerBoolean debug = new LoggerBoolean(LOG.isDebugEnabled());
    private static final LoggerBoolean trace = new LoggerBoolean(LOG.isTraceEnabled());
    static {
        LoggerUtil.attachObserver(LOG, debug, trace);
    }

    private final HStoreSite hstore_site;
    
    private final ProfileMeasurement idleTime = new ProfileMeasurement("IDLE");
    private final ProfileMeasurement execTime = new ProfileMeasurement("EXEC");
    
    
    /**
     * Whether we should stop processing our queue
     */
    private boolean stop = false;
    
    /**
     * ClientResponses that can be immediately returned to the client
     */
    private final LinkedBlockingDeque<Pair<LocalTransaction, ClientResponseImpl>> queue;

    /**
     * Handle to ourselves
     */
    private Thread self = null; 
    
    /**
     * 
     * @param hstore_site
     */
    public PartitionExecutorPostProcessor(HStoreSite hstore_site,
                                          LinkedBlockingDeque<Pair<LocalTransaction, ClientResponseImpl>> queue) {
        this.hstore_site = hstore_site;
        this.queue = queue;
    }
    
    @Override
    public void run() {
        this.self = Thread.currentThread();
        this.self.setName(HStoreThreadManager.getThreadName(hstore_site, "post"));
        if (hstore_site.getHStoreConf().site.cpu_affinity) {
            hstore_site.getThreadManager().registerProcessingThread();
        }
        if (debug.get())
            LOG.debug("Starting transaction post-processing thread");
        
        HStoreConf hstore_conf = hstore_site.getHStoreConf();
        Pair<LocalTransaction, ClientResponseImpl> pair = null;
        while (this.stop == false) {
            try {
                if (hstore_conf.site.status_show_executor_info) idleTime.start();
                pair = this.queue.takeFirst();
                if (hstore_conf.site.status_show_executor_info) idleTime.stop();
            } catch (InterruptedException ex) {
                this.stop = true;
                break;
            }
            LocalTransaction ts = pair.getFirst();
            assert(ts != null);
            ClientResponseImpl cr = pair.getSecond();
            assert(cr != null);
            
            if (hstore_conf.site.status_show_executor_info) execTime.start();
            if (debug.get()) LOG.debug(String.format("Processing ClientResponse for %s at partition %d [status=%s]",
                                                     ts, ts.getBasePartition(), cr.getStatus()));
            try {
                hstore_site.sendClientResponse(ts, cr);
                hstore_site.deleteTransaction(ts.getTransactionId(), cr.getStatus());
            } catch (Throwable ex) {
                LOG.error(String.format("Failed to process %s properly\n%s", ts, cr));
                if (this.isShuttingDown() == false) throw new RuntimeException(ex);
                break;
            }
            if (hstore_conf.site.status_show_executor_info) execTime.stop();
        } // WHILE
    }
    
    @Override
    public boolean isShuttingDown() {
        return (this.stop);
    }
    
    @Override
    public void prepareShutdown(boolean error) {
        this.queue.clear();
    }
    
    @Override
    public void shutdown() {
        if (debug.get())
            LOG.debug(String.format("Transaction Post-Processing Thread Idle Time: %.2fms", idleTime.getTotalThinkTimeMS()));
        this.stop = true;
        if (this.self != null) this.self.interrupt();
    }

    public ProfileMeasurement getIdleTime() {
        return (this.idleTime);
    }
    public ProfileMeasurement getExecTime() {
        return (this.execTime);
    }
    
}
