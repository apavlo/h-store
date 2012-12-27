package edu.brown.hstore.util;

import java.util.Collection;
import java.util.Iterator;
import java.util.Queue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;

import edu.brown.logging.LoggerUtil;
import edu.brown.logging.LoggerUtil.LoggerBoolean;
import edu.brown.profilers.ProfileMeasurement;
import edu.brown.profilers.ProfileMeasurementUtil;

/**
 * Creates a wrapper around a queue that provides a dynamic limit on the
 * size of the queue. You can specify whether this limit is allowed to increase,
 * how much it should increase by, and what the max limit is.
 * You can attach this wrapper to an EventObservable that will tell us
 * when it is time to increase the limit.
 * @author pavlo
 * @param <E>
 */
public class ThrottlingQueue<E> implements BlockingQueue<E> {
    private static final Logger LOG = Logger.getLogger(ThrottlingQueue.class);
    private static final LoggerBoolean debug = new LoggerBoolean(LOG.isDebugEnabled());
    private static final LoggerBoolean trace = new LoggerBoolean(LOG.isTraceEnabled());
    static {
        LoggerUtil.attachObserver(LOG, debug, trace);
    }
    
    private final BlockingQueue<E> queue;
    private volatile int size;
    
    private boolean throttled;
    private int queue_max;
    private int queue_release;
    private double queue_release_factor;
    private int queue_increase;
    private int queue_increase_max;
    private final ProfileMeasurement throttle_time;
    private boolean allow_increase;
         
    /**
     * Constructor
     * @param queue
     * @param queue_max
     * @param queue_release
     * @param queue_increase
     * @param queue_increase_max
     */
    public ThrottlingQueue(BlockingQueue<E> queue,
                            int queue_max,
                            double queue_release,
                            int queue_increase,
                            int queue_increase_max) {
        this.queue = queue;
        this.throttled = false;
        
        this.setQueueMax(queue_max);
        this.setQueueIncrease(queue_increase);
        this.setQueueIncreaseMax(queue_increase_max);
        this.setQueueReleaseFactor(queue_release);
        
        this.throttle_time = new ProfileMeasurement("throttling");
//        if (hstore_site.getHStoreConf().site.status_show_executor_info) {
//            this.throttle_time.resetOnEvent(hstore_site.getStartWorkloadObservable());
//        }
    }

    public void setQueueIncrease(int queue_increase) {
        this.queue_increase = queue_increase;
        this.allow_increase = (this.queue_increase > 0);
    }
    
    public void setQueueIncreaseMax(int queue_increase_max) {
        this.queue_increase_max = queue_increase_max;
    }
    
    public void setQueueMax(int queue_max) {
        this.queue_max = queue_max;
    }
    
    public void setQueueReleaseFactor(double queue_release_factor) {
        this.queue_release_factor = queue_release_factor;
        this.queue_release = Math.max((int)(this.queue_max * this.queue_release_factor), 1);
    }
    
    public ProfileMeasurement getThrottleTime() {
        return (this.throttle_time);
    }
    protected final Queue<E> getQueue() {
        return (this.queue);
    }
    public boolean isThrottled() {
        return (this.throttled);
    }
    public int getQueueMax() {
        return (this.queue_max);
    }
    public int getQueueRelease() {
        return (this.queue_release);
    }
    public int getQueueIncrease() {
        return (this.queue_increase);
    }
    
    /**
     * Check whether the size of this queue is greater than our max limit
     * We don't need to worry if this is 100% accurate, so we won't block here
     */
    public void checkThrottling(boolean increase) {
        // If they're not throttled, then we should check whether
        // we need to throttle them
        if (this.throttled == false) {
            // If they've gone above the current queue max size, then
            // they are throtttled!
            if (this.size >= this.queue_max)
                this.throttled = true;
            
            // Or if the queue is completely empty and we're allowe to increase
            // the max limit, then we'll go ahead and do that for them here
            else if (increase && this.size == 0) {
                synchronized (this) {
                    this.queue_max = Math.min(this.queue_increase_max, (this.queue_max + this.queue_increase));
                    this.queue_release = Math.max((int)(this.queue_max * this.queue_release_factor), 1);
                } // SYNCH
            }
        }
        // If we're throttled and we've gone below our release
        // threshold, then we can go ahead and unthrottle them
        else if (this.size <= this.queue_release) {
            if (debug.get()) LOG.debug(String.format("Unthrottling queue [size=%d > release=%d]",
                                                     this.size, this.queue_release));
            this.throttled = false;
        }
    }
    
    @Override
    public boolean add(E e) {
        return (this.offer(e, false));
    }
    @Override
    public boolean offer(E e) {
        return this.offer(e, false);
    }
    /**
     * Offer an element to the queue. If force is true,
     * then it will always be inserted and not count against
     * the throttling counter.
     * @param e
     * @param force
     * @return
     */
    public boolean offer(E e, boolean force) {
        boolean ret = false;
        if (force) {
            this.queue.add(e);
            ret = true;
        } else if (this.throttled == false) {
            ret = this.queue.offer(e);
        }
        if (ret && force == false) {
            this.size = this.queue.size();
            this.checkThrottling(this.allow_increase);
        }
        return (ret);
    }
    
    @Override
    public void put(E e) throws InterruptedException {
        boolean ret = this.queue.offer(e);
        if (ret) {
            this.size = this.queue.size();
            this.checkThrottling(this.allow_increase);
        }
        return;
    }

    @Override
    public boolean offer(E e, long timeout, TimeUnit unit) throws InterruptedException {
        boolean ret = this.queue.offer(e, timeout, unit);
        if (ret) {
            this.size = this.queue.size();
            this.checkThrottling(this.allow_increase);
        }
        return (ret);
    }
    
    @Override
    public boolean remove(Object o) {
        boolean ret = this.queue.remove(o);
        if (ret) {
            this.size = this.queue.size();
            this.checkThrottling(this.allow_increase);
        }
        return (ret);
    }
    @Override
    public E poll() {
        E e = this.queue.poll();
        if (e != null) {
            this.size = this.queue.size();
            this.checkThrottling(this.allow_increase);
        }
        return (e);
    }

    @Override
    public E poll(long timeout, TimeUnit unit) throws InterruptedException {
        E e = this.queue.poll(timeout, unit);
        if (e != null) {
            this.size = this.queue.size();
            this.checkThrottling(this.allow_increase);
        }
        return (e);
    }
    
    @Override
    public E remove() {
        E e = this.queue.remove();
        if (e != null) {
            this.size = this.queue.size();
            this.checkThrottling(this.allow_increase);
        }
        return (e);
    }

    @Override
    public E take() throws InterruptedException {
        E e = this.queue.take();
        if (e != null) {
            this.size = this.queue.size();
            this.checkThrottling(this.allow_increase);
        }
        return (e);
    }

    @Override
    public int remainingCapacity() {
        return (this.queue.remainingCapacity());
    }

    @Override
    public int drainTo(Collection<? super E> c) {
        int ret = this.queue.drainTo(c);
        if (ret > 0) {
            this.size -= ret;
            this.checkThrottling(this.allow_increase);
        }
        return (ret);
    }

    @Override
    public int drainTo(Collection<? super E> c, int maxElements) {
        int ret = this.queue.drainTo(c, maxElements);
        if (ret > 0) {
            this.size = this.queue.size();
            this.checkThrottling(this.allow_increase);
        }
        return (ret);
    }
    
    @Override
    public void clear() {
        if (this.throttled) ProfileMeasurementUtil.stop(true, this.throttle_time);
        this.throttled = false;
        this.size = 0;
        this.queue.clear();
    }
    @Override
    public boolean removeAll(Collection<?> c) {
        boolean ret = this.queue.removeAll(c);
        if (ret) this.size = 0;
        return (ret);
    }
    @Override
    public boolean retainAll(Collection<?> c) {
        boolean ret = this.queue.retainAll(c);
        if (ret) this.size = this.queue.size();
        return (ret);
    }
    @Override
    public boolean addAll(Collection<? extends E> c) {
        boolean ret = this.queue.addAll(c);
        if (ret) this.size = this.queue.size();
        return (ret);
    }
    @Override
    public E element() {
        return (this.queue.element());
    }
    @Override
    public E peek() {
        return (this.queue.peek());
    }
    @Override
    public boolean contains(Object o) {
        return (this.queue.contains(o));
    }
    @Override
    public boolean containsAll(Collection<?> c) {
        return (this.queue.containsAll(c));
    }
    @Override
    public boolean isEmpty() {
        return (this.queue.isEmpty());
    }
    @Override
    public Iterator<E> iterator() {
        return (this.queue.iterator());
    }
    @Override
    public int size() {
        return (this.size);
    }
    @Override
    public Object[] toArray() {
        return (this.queue.toArray());
    }
    @Override
    public <T> T[] toArray(T[] a) {
        return (this.queue.toArray(a));
    }

    @Override
    public String toString() {
        return String.format("%s [max=%d / release=%d / increase=%d]",
                             this.queue.toString(),
                             this.queue_max, this.queue_release, this.queue_increase);
    }

    public void setAllowIncrease(boolean allow_increase) {
        this.allow_increase = allow_increase;
    }
}
