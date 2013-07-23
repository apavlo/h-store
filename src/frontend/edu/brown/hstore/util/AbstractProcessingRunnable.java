package edu.brown.hstore.util;

import java.util.concurrent.BlockingQueue;

import org.apache.log4j.Logger;

import edu.brown.hstore.HStoreSite;
import edu.brown.hstore.HStoreThreadManager;
import edu.brown.hstore.conf.HStoreConf;
import edu.brown.interfaces.Shutdownable;
import edu.brown.logging.LoggerUtil;
import edu.brown.logging.LoggerUtil.LoggerBoolean;
import edu.brown.profilers.ProfileMeasurement;
import edu.brown.profilers.ProfileMeasurementUtil;
import edu.brown.utils.ExceptionHandlingRunnable;

public abstract class AbstractProcessingRunnable<E> extends ExceptionHandlingRunnable implements Shutdownable {
    private static final Logger LOG = Logger.getLogger(AbstractProcessingRunnable.class);
    private static final LoggerBoolean debug = new LoggerBoolean();
    private static final LoggerBoolean trace = new LoggerBoolean();
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
    
    
    public AbstractProcessingRunnable(HStoreSite hstore_site, String name, BlockingQueue<E> queue) {
        this(hstore_site, name, queue, false);
    }
    
    public AbstractProcessingRunnable(HStoreSite hstore_site, String name, BlockingQueue<E> queue, boolean profile) {
        assert(queue != null);
        
        this.hstore_site = hstore_site;
        this.hstore_conf = hstore_site.getHStoreConf();
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
    public final void runImpl() {
        this.self = Thread.currentThread();
        this.self.setName(HStoreThreadManager.getThreadName(hstore_site, this.name));
        this.hstore_site.getThreadManager().registerProcessingThread();

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
                    ProfileMeasurementUtil.swap(idleTime, execTime);
                }
            }
            try {
                if (next != null) this.processingCallback(next);
            } finally {
                if (execTime != null) execTime.stop();                
            }
        } // WHILE
    }

    /**
     * Special callback for when the processing thread gets a new entry in its queue
     * If the implementing class should invoke shutdown() if it needs to stop the processing.
     * Otherwise the processing thread will immediately go back to queue and wait
     * for another entry 
     * @param next
     */
    protected abstract void processingCallback(E next);
    
    /**
     * Special callback for when an entry is removed from the processing queue.
     * This will be invoked by prepareShutdown()
     * @param next
     */
    protected void removeCallback(E next) {
        // The default is to do nothing!
    }
    
    @Override
    public final void prepareShutdown(boolean error) {
        E next = null;
        while ((next = this.queue.poll()) != null) {
            this.removeCallback(next);
        } // WHILE
    }

    @Override
    public final void shutdown() {
        this.stop = true;
        if (this.self != null) this.self.interrupt();
        if (debug.val && this.idleTime != null)
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
