package edu.brown.hstore.dispatchers;

import java.util.Arrays;
import java.util.concurrent.LinkedBlockingDeque;

import org.apache.log4j.Logger;

import edu.brown.hstore.HStoreCoordinator;
import edu.brown.hstore.conf.HStoreConf;
import edu.brown.utils.ClassUtil;
import edu.brown.utils.ProfileMeasurement;

/**
 * A dispatcher is a asynchronous processor for a specific type of message from
 * the HStoreCoordinator 
 * @author pavlo
 * @param <E>
 */
public abstract class AbstractDispatcher<E> implements Runnable {
    private static final Logger LOG = Logger.getLogger(AbstractDispatcher.class);
    
    protected final HStoreCoordinator hstore_coordinator;
    private final ProfileMeasurement idleTime = new ProfileMeasurement("IDLE");
    private final LinkedBlockingDeque<E> queue = new LinkedBlockingDeque<E>();

    
    /**
     * @param hStoreCoordinator
     */
    public AbstractDispatcher(HStoreCoordinator hStoreCoordinator) {
        this.hstore_coordinator = hStoreCoordinator;
    }
    
    @Override
    public final void run() {
        HStoreConf hstore_conf = this.hstore_coordinator.getHStoreConf(); 
        this.hstore_coordinator.getHStoreSite().getThreadManager().registerProcessingThread();
        
        E e = null;
        boolean profiling = hstore_conf.site.exec_profiling;
        while (this.hstore_coordinator.isShutdownOrPrepareShutDown() == false) {
            try {
                if (profiling) idleTime.start();
                e = this.queue.take();
                if (profiling) idleTime.stop();
            } catch (InterruptedException ex) {
                break;
            }
            try {
                this.runImpl(e);
            } catch (Throwable ex) {
                String dump = null;
                if (ClassUtil.isArray(e)) {
                    dump = Arrays.toString((Object[])e);
                } else if (e != null) {
                    dump = e.toString();
                }
                LOG.warn("Failed to process queued element: " + dump, ex);
                continue;
            }
        } // WHILE
    }
    public void queue(E e) {
        this.queue.offer(e);
    }
    public ProfileMeasurement getIdleTime() {
        return (this.idleTime);
    }
    
    public abstract void runImpl(E e);
    
}