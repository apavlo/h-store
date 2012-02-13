package edu.brown.hstore.dispatchers;

import java.util.concurrent.LinkedBlockingDeque;

import org.apache.log4j.Logger;

import edu.brown.hstore.HStoreCoordinator;
import edu.brown.utils.ProfileMeasurement;

/**
 * A dispatcher is a asynchronous processor for a specific type of message from
 * the HStoreCoordinator 
 * 
 * @author pavlo
 * @param <E>
 */
public abstract class AbstractDispatcher<E> implements Runnable {
    private static final Logger LOG = Logger.getLogger(AbstractDispatcher.class);
    
    protected final HStoreCoordinator hStoreCoordinator;
    private final ProfileMeasurement idleTime = new ProfileMeasurement("IDLE");
    private final LinkedBlockingDeque<E> queue = new LinkedBlockingDeque<E>();

    
    /**
     * @param hStoreCoordinator
     */
    public AbstractDispatcher(HStoreCoordinator hStoreCoordinator) {
        this.hStoreCoordinator = hStoreCoordinator;
    }
    
    @Override
    public final void run() {
        if (this.hStoreCoordinator.getHStoreConf().site.cpu_affinity)
            this.hStoreCoordinator.getHStoreSite().getThreadManager().registerProcessingThread();
        E e = null;
        while (this.hStoreCoordinator.isShutdownOrPrepareShutDown() == false) {
            try {
                idleTime.start();
                e = this.queue.take();
                idleTime.stop();
            } catch (InterruptedException ex) {
                break;
            }
            try {
                this.runImpl(e);
            } catch (Throwable ex) {
                LOG.warn("Failed to process queued element " + e, ex);
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