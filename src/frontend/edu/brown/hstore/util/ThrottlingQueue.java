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
    private int throttleThreshold;
    private int throttleRelease;
    private double throttleReleaseFactor;
    private int throttleThresholdIncrease;
    private int throttleThresholdMaxSize;
    private boolean allow_increase = false;
    
    private final ProfileMeasurement throttle_time;
         
    /**
     * Constructor
     * @param queue The original queue
     * @param throttleThreshold The initial max size of the queue before it is throttled.
     * @param throttleReleaseFactor The release factor for when the queue will be unthrottled.
     * @param throttleThresholdIncrease The increase delta for when we will increase the max size.
     * @param throttleThresholdMaxSize The maximum size of the queue.
     */
    public ThrottlingQueue(BlockingQueue<E> queue,
                           int throttleThreshold,
                           double throttleReleaseFactor,
                           int throttleThresholdIncrease,
                           int throttleThresholdMaxSize) {
        this.queue = queue;
        this.throttled = false;
        
        this.throttleThreshold = throttleThreshold;
        this.throttleReleaseFactor = throttleReleaseFactor;
        this.throttleThresholdIncrease = throttleThresholdIncrease;
        this.throttleThresholdMaxSize = throttleThresholdMaxSize;
        this.computeReleaseThreshold();
        
        this.throttle_time = new ProfileMeasurement("THROTTLING");
    }
    
    private void computeReleaseThreshold() {
        this.throttleRelease = Math.max((int)(this.throttleThreshold * this.throttleReleaseFactor), 1);
    }
    
    // ----------------------------------------------------------------------------
    // UTILITY METHODS
    // ----------------------------------------------------------------------------

    /**
     * Returns true if this queue is currently throttled.
     * @return
     */
    public boolean isThrottled() {
        return (this.throttled);
    }
    
    /**
     * Get the maximum size of the queue before it will throttle new additions.
     * @return
     */
    public int getThrottleThreshold() {
        return (this.throttleThreshold);
    }
    /**
     * Set the maximum size of the queue before it will throttle new additions.
     * @param size
     */
    public void setThrottleThreshold(int size) {
        assert(size >= 0);
        this.throttleThreshold = size;
        this.computeReleaseThreshold();
    }
    
    public void setThrottleReleaseFactor(double queue_release_factor) {
        this.throttleReleaseFactor = queue_release_factor;
        this.computeReleaseThreshold();
    }
    public double getThrottleReleaseFactor() {
        return (this.throttleReleaseFactor);
    }
    /**
     * Return the size of the queue that much be reached before a 
     * throttled queue will be allowed to take new additions.
     * @return
     */
    public int getThrottleRelease() {
        return (this.throttleRelease);
    }
    
    /**
     * Return the value that this ThrottlingQueue will automatically increase
     * the throttle threshold if the size of this queue goes to zero.
     * This will only matter if allow_increase is set to true.
     * @return
     */
    public int getThrottleThresholdIncreaseDelta() {
        return (this.throttleThresholdIncrease);
    }
    /**
     * Set the delta that the queue will automatically increase the throttle
     * threshold if the size of this queue goes to zero. 
     * This will only matter if allow_increase is set to true.
     * @param delta
     */
    public void setThrottleThresholdIncreaseDelta(int delta) {
        this.throttleThresholdIncrease = delta;
    }
    
    /**
     * Set the max size of the throttling threshold. If this queue is allowed
     * to increase the threshold whenever the queue is empty, then this is
     * the maximum size that the threshold will be allowed to increase to.
     * @param size
     */
    public void setThrottleThresholdMaxSize(int size) {
        this.throttleThresholdMaxSize = size;
    }
    public int getThrottleThresholdMaxSize() {
        return (this.throttleThresholdMaxSize);
    }
    
    /**
     * If the allow_increase flag is set to true, then the
     * throttle threshold will be allowed to automatically grow
     * whenever the the size of the queue is zero.
     * @param allow_increase
     */
    public void setAllowIncrease(boolean allow_increase) {
        this.allow_increase = allow_increase;
    }
    
    protected final Queue<E> getQueue() {
        return (this.queue);
    }
    
    public ProfileMeasurement getThrottleTime() {
        return (this.throttle_time);
    }
    
    // ----------------------------------------------------------------------------
    // THROTTLING QUEUE API METHODS
    // ----------------------------------------------------------------------------
    
    /**
     * Offer an element to the queue. If force is true, then the element will
     * always be inserted and not count against the throttling counter.
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
            if (this.size >= this.throttleThreshold)
                this.throttled = true;
            
            // Or if the queue is completely empty and we're allowe to increase
            // the max limit, then we'll go ahead and do that for them here
            else if (increase && this.size == 0) {
                synchronized (this) {
                    this.throttleThreshold = Math.min(this.throttleThresholdMaxSize, (this.throttleThreshold + this.throttleThresholdIncrease));
                    this.throttleRelease = Math.max((int)(this.throttleThreshold * this.throttleReleaseFactor), 1);
                } // SYNCH
            }
        }
        // If we're throttled and we've gone below our release
        // threshold, then we can go ahead and unthrottle them
        else if (this.size <= this.throttleRelease) {
            if (debug.val)
                LOG.debug(String.format("Unthrottling queue [size=%d > release=%d]",
                          this.size, this.throttleRelease));
            this.throttled = false;
        }
    }
    
    // ----------------------------------------------------------------------------
    // QUEUE API METHODS
    // ----------------------------------------------------------------------------
    
    @Override
    public boolean add(E e) {
        return (this.offer(e, false));
    }
    @Override
    public boolean offer(E e) {
        return this.offer(e, false);
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
        if (this.throttled) this.throttle_time.stopIfStarted();
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
                             this.throttleThreshold, this.throttleRelease, this.throttleThresholdIncrease);
    }
}
