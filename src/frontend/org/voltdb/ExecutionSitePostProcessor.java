package org.voltdb;

import java.util.Arrays;
import java.util.concurrent.LinkedBlockingDeque;

import org.apache.log4j.Logger;

import edu.brown.utils.LoggerUtil;
import edu.brown.utils.ProfileMeasurement;
import edu.brown.utils.LoggerUtil.LoggerBoolean;
import edu.mit.hstore.HStoreConf;
import edu.mit.hstore.HStoreSite;
import edu.mit.hstore.dtxn.LocalTransactionState;
import edu.mit.hstore.interfaces.Shutdownable;

public final class ExecutionSitePostProcessor implements Runnable, Shutdownable {
    private static final Logger LOG = Logger.getLogger(ExecutionSitePostProcessor.class);
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
    private final LinkedBlockingDeque<Object[]> queue;

    /**
     * Handle to ourselves
     */
    private Thread self = null; 
    
    /**
     * 
     * @param hstore_site
     */
    public ExecutionSitePostProcessor(HStoreSite hstore_site, LinkedBlockingDeque<Object[]> queue) {
        this.hstore_site = hstore_site;
        this.queue = queue;
    }
    
    @Override
    public void run() {
        this.self = Thread.currentThread();
        this.self.setName(HStoreSite.getThreadName(hstore_site, "post"));
        if (hstore_site.getHStoreConf().site.cpu_affinity) {
            hstore_site.getThreadManager().registerProcessingThread();
        }
        if (debug.get())
            LOG.debug("Starting transaction post-processing thread");
        
        HStoreConf hstore_conf = hstore_site.getHStoreConf();
        Object triplet[] = null;
        while (this.stop == false) {
            try {
                if (hstore_conf.site.status_show_executor_info) idleTime.start();
                triplet = this.queue.takeFirst();
                if (hstore_conf.site.status_show_executor_info) idleTime.stop();
                assert(triplet != null);
                assert(triplet.length == 3) : "Unexpected response: " + Arrays.toString(triplet);
            } catch (InterruptedException ex) {
                this.stop = true;
                break;
            }
            if (hstore_conf.site.status_show_executor_info) execTime.start();
            ExecutionSite es = (ExecutionSite)triplet[0];
            LocalTransactionState ts = (LocalTransactionState)triplet[1];
            ClientResponseImpl cr = (ClientResponseImpl)triplet[2];
            if (debug.get()) LOG.debug(String.format("Processing ClientResponse for %s at partition %d [status=%s]",
                                           ts, es.getPartitionId(), cr.getStatusName()));
            try {
                es.processClientResponse(ts, cr);
            } catch (Throwable ex) {
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
    public void prepareShutdown() {
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
