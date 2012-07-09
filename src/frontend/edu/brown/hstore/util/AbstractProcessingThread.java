package edu.brown.hstore.util;

import java.util.concurrent.BlockingQueue;

import org.apache.log4j.Logger;

import edu.brown.hstore.HStoreConstants;
import edu.brown.hstore.HStoreSite;
import edu.brown.hstore.HStoreThreadManager;
import edu.brown.hstore.conf.HStoreConf;
import edu.brown.hstore.interfaces.Shutdownable;
import edu.brown.logging.LoggerUtil;
import edu.brown.logging.LoggerUtil.LoggerBoolean;
import edu.brown.utils.ProfileMeasurement;

public abstract class AbstractProcessingThread<E> implements Runnable, Shutdownable {
    private static final Logger LOG = Logger.getLogger(AbstractProcessingThread.class);
    private static final LoggerBoolean debug = new LoggerBoolean(LOG.isDebugEnabled());
    private static final LoggerBoolean trace = new LoggerBoolean(LOG.isTraceEnabled());
    static {
        LoggerUtil.attachObserver(LOG, debug, trace);
    }
    
    protected final HStoreSite hstore_site;
    protected final String name;
    protected final BlockingQueue<E> queue;
    protected boolean stop = false;
    protected Thread self;
    protected HStoreConf hstore_conf;
    
    protected final ProfileMeasurement idleTime;
    protected final ProfileMeasurement execTime;
    
    
    public AbstractProcessingThread(HStoreSite hstore_site, String name, BlockingQueue<E> queue) {
        this(hstore_site, name, queue, false);
    }
    
    public AbstractProcessingThread(HStoreSite hstore_site, String name, BlockingQueue<E> queue, boolean profile) {
        assert(queue != null);
        
        this.hstore_site = hstore_site;
        this.name = name;
        this.queue = queue;
        
        
        if (profile) {
            this.idleTime = new ProfileMeasurement("IDLE");
            this.execTime = new ProfileMeasurement("EXEC");
        } else {
            this.idleTime = null;
            this.execTime = null;
        }
    }
    
    @Override
    public final void run() {
        this.self = Thread.currentThread();
        this.self.setName(HStoreThreadManager.getThreadName(hstore_site, HStoreConstants.THREAD_NAME_ANTICACHE));
        this.hstore_site.getThreadManager().registerProcessingThread();
        this.hstore_conf = hstore_site.getHStoreConf();

        E next = null;
        while (this.stop == false) {
            try {
                if (idleTime != null) idleTime.start();
                next = this.queue.take();
                if (idleTime != null) idleTime.stop();
            } catch (InterruptedException ex) {
                this.stop = true;
                break;
            } finally {
                if (this.stop == false && idleTime != null) {
                    ProfileMeasurement.swap(idleTime, execTime);
                }
            }
            try {
                if (next != null) this.processingCallback(next);
            } finally {
                if (execTime != null) execTime.stop();                
            }
        } // WHILE
    }

    public abstract void processingCallback(E next);
    
    @Override
    public final void prepareShutdown(boolean error) {
        // TODO Auto-generated method stub

    }

    @Override
    public final void shutdown() {
        this.stop = true;
        if (this.self != null) this.self.interrupt();
        if (debug.get() && this.idleTime != null)
            LOG.debug(String.format("%s Idle Time: %.2fms",
                                    this.getClass().getSimpleName(),
                                    idleTime.getTotalThinkTimeMS()));
    }

    @Override
    public final boolean isShuttingDown() {
        return (this.stop);
    }
    
    public final ProfileMeasurement getIdleTime() {
        return (this.idleTime);
    }
    public final ProfileMeasurement getExecTime() {
        return (this.execTime);
    }
    
}
