package edu.brown.hstore.util;

import java.util.Collection;
import java.util.Iterator;
import java.util.Queue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;

import edu.brown.hstore.dtxn.AbstractTransaction;
import edu.brown.logging.LoggerUtil;
import edu.brown.logging.LoggerUtil.LoggerBoolean;
import edu.brown.utils.EventObservable;
import edu.brown.utils.EventObserver;
import edu.brown.utils.ProfileMeasurement;

/**
 * Creates a wrapper around a queue that provides a dynamic limit on the
 * size of the queue. You can specify whether this limit is allowed to increase,
 * how much it should increase by, and what the max limit is.
 * You can attach this wrapper to an EventObservable that will tell us
 * when it is time to increase the limit.
 * @author pavlo
 * @param <E>
 */
public class ThrottlingQueue<E> extends EventObserver<AbstractTransaction> implements BlockingQueue<E> {
    public static final Logger LOG = Logger.getLogger(ThrottlingQueue.class);
    private static final LoggerBoolean debug = new LoggerBoolean(LOG.isDebugEnabled());
    private static final LoggerBoolean trace = new LoggerBoolean(LOG.isTraceEnabled());
    static {
        LoggerUtil.attachObserver(LOG, debug, trace);
    }
    
    private final Queue<E> queue;
    
    private volatile int size;
    private boolean throttled;
    private int queue_max;
    private int queue_release;
    private double queue_release_factor;
    private final int queue_increase;
    private final int queue_increase_max;
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
    public ThrottlingQueue(Queue<E> queue, int queue_max, double queue_release, int queue_increase, int queue_increase_max) {
        this.queue = queue;
        this.throttled = false;
        this.queue_max = queue_max;
        this.queue_increase = queue_increase;
        this.queue_increase_max = queue_increase_max;
        this.queue_release_factor = queue_release;
        this.queue_release = Math.max((int)(this.queue_max * this.queue_release_factor), 1);
        this.allow_increase = (this.queue_increase > 0);
        this.throttle_time = new ProfileMeasurement("throttling");
//        if (hstore_site.getHStoreConf().site.status_show_executor_info) {
//            this.throttle_time.resetOnEvent(hstore_site.getStartWorkloadObservable());
//        }
    }

    public ProfileMeasurement getThrottleTime() {
        return (this.throttle_time);
    }
    public Queue<E> getQueue() {
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
        if (this.throttled == false) {
            if (this.size > this.queue_max) this.throttled = true;
            else if (increase && this.size == 0) {
                this.queue_max = Math.min(this.queue_increase_max, (this.queue_max + this.queue_increase));
                this.queue_release = Math.max((int)(this.queue_max * this.queue_release_factor), 1);
            }
        }
        else if (this.throttled && this.size > this.queue_release) {
            if (debug.get()) LOG.debug(String.format("Unthrottling queue [size=%d > release=%d]",
                                                     this.size, this.queue_release));
            this.throttled = false;
        }
    }
    
    @Override
    public boolean add(E e) {
        return (this.offer(e));
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
        boolean ret;
        if (force) {
            ret = this.queue.offer(e);
        } else if (this.throttled) {
            return (false);
        } else {
            ret = this.queue.offer(e);    
        }
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
            this.checkThrottling(this.allow_increase);
        }
        return (ret);
    }
    @Override
    public E poll() {
        E e = this.queue.poll();
        if (e != null) {
            this.checkThrottling(this.allow_increase);
        }
        return (e);
    }
    @Override
    public E remove() {
        E e = this.queue.remove();
        if (e != null) {
            this.checkThrottling(this.allow_increase);
        }
        return (e);
    }
    

    @Override
    public void put(E e) throws InterruptedException {
        // TODO Auto-generated method stub
        
    }

    @Override
    public boolean offer(E e, long timeout, TimeUnit unit) throws InterruptedException {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public E take() throws InterruptedException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public E poll(long timeout, TimeUnit unit) throws InterruptedException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public int remainingCapacity() {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public int drainTo(Collection<? super E> c) {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public int drainTo(Collection<? super E> c, int maxElements) {
        // TODO Auto-generated method stub
        return 0;
    }
    
    
    @Override
    public void clear() {
        if (this.throttled) ProfileMeasurement.stop(false, this.throttle_time);
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
//        if (ret) this.size = this.queue.size();
        return (ret);
    }
    @Override
    public boolean addAll(Collection<? extends E> c) {
        boolean ret = this.queue.addAll(c);
//        if (ret) this.size = this.queue.size();
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
        return (this.queue.size());
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

    @Override
    public void update(EventObservable<AbstractTransaction> o, AbstractTransaction arg) {
        this.allow_increase = true;
    }
}
