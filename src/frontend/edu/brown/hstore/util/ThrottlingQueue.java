package edu.brown.hstore.util;

import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.log4j.Logger;

import edu.brown.logging.LoggerUtil;
import edu.brown.logging.LoggerUtil.LoggerBoolean;
import edu.brown.profilers.ProfileMeasurement;
import edu.brown.utils.StringUtil;

/**
 * Creates a wrapper around a queue that provides a dynamic limit on the
 * size of the queue. You can specify whether this limit is allowed to increase,
 * how much it should increase by, and what the max limit is.
 * You can attach this wrapper to an EventObservable that will tell us
 * when it is time to increase the limit.
 * @author pavlo
 * @param <E>
 */
public class ThrottlingQueue<E> implements Queue<E> {
    private static final Logger LOG = Logger.getLogger(ThrottlingQueue.class);
    private static final LoggerBoolean debug = new LoggerBoolean();
    private static final LoggerBoolean trace = new LoggerBoolean();
    static {
        LoggerUtil.attachObserver(LOG, debug, trace);
    }
    
    // ----------------------------------------------------------------------------
    // INTERNAL DATA MEMBERS
    // ----------------------------------------------------------------------------
    
    /**
     * The internal queue that we're going to wrap this ThrottlingQueue around.
     */
    private final Queue<E> queue;
    
    /**
     * This is a thread-safe approximation of what the current size of the
     * queue. It's not meant to be completely accurate, but just good enough
     * to get an idea of whether the queue is overloaded.
     */
    private final AtomicInteger size = new AtomicInteger(0);
    
    /**
     * If this flag is set to true, then this queue is currently throttled.
     * It will not be allowed to take in new elements until the size goes
     * below the throttleRelease,
     */
    private boolean throttled;
    private int origThrottleThreshold;
    private int throttleThreshold;
    private int throttleRelease;
    private double throttleReleaseFactor;
    
    private int autoDelta;
    private int autoMinSize;
    private int autoMaxSize;
    
    /**
     * If this flag is set to true, then the ThrottlingQueue will automatically
     * increase the throttleThreshold by throttleThresholdIncrease any time the 
     * queue is completely empty.
     */
    private boolean allowIncreaseOnZero = false;
    
    /**
     * If this flag is set to true, then the ThrottlingQueue will automatically
     * decrease the throttleThreshold by throttleThresholdDelta any time the 
     * queue gets throttled.
     */
    private boolean allowDecreaseOnThrottle = false;
    
    private final ProfileMeasurement throttle_time = new ProfileMeasurement("THROTTLING");
    private boolean throttle_time_enabled = false;
         
    // ----------------------------------------------------------------------------
    // CONSTRUCTORS
    // ----------------------------------------------------------------------------
    
    /**
     * Constructor
     * @param queue The original queue
     * @param throttleThreshold The initial max size of the queue before it is throttled.
     * @param throttleReleaseFactor The release factor for when the queue will be unthrottled.
     * @param autoDelta The increase delta for when we will increase the max size.
     * @param autoMaxSize The maximum size of the queue.
     */
    public ThrottlingQueue(Queue<E> queue,
                           int throttleThreshold,
                           double throttleReleaseFactor,
                           int autoDelta,
                           int autoMinSize,
                           int autoMaxSize) {
        this.queue = queue;
        this.throttled = false;
        
        this.throttleThreshold = throttleThreshold;
        this.origThrottleThreshold = throttleThreshold;
        this.throttleReleaseFactor = throttleReleaseFactor;
        this.autoDelta = autoDelta;
        this.autoMinSize = autoMinSize;
        this.autoMaxSize = autoMaxSize;
        this.computeReleaseThreshold();
        this.checkThrottling(this.allowDecreaseOnThrottle, this.size());
    }
    
    /**
     * Constructor with threshold auto-increase disbled.
     * @param queue The original queue
     * @param throttleThreshold The initial max size of the queue before it is throttled.
     * @param throttleReleaseFactor The release factor for when the queue will be unthrottled.
     */
    public ThrottlingQueue(Queue<E> queue,
                           int throttleThreshold,
                           double throttleReleaseFactor) {
        this(queue, throttleThreshold, throttleReleaseFactor, 0, throttleThreshold, throttleThreshold);
        this.allowIncreaseOnZero = false;
        this.allowDecreaseOnThrottle = false;
    }

    // ----------------------------------------------------------------------------
    // INTERNAL METHODS
    // ----------------------------------------------------------------------------
    
    private void computeReleaseThreshold() {
        this.throttleRelease = Math.max((int)(this.throttleThreshold * this.throttleReleaseFactor), 1);
    }
    
    /**
     * Check whether the size of this queue is greater than our max limit.
     * We don't need to worry if this is 100% accurate, so we won't block here
     * @param can_change
     * @param last_size
     */
    private void checkThrottling(boolean can_change, int last_size) {
        
        // If they're not throttled, then we should check whether
        // we need to throttle them
        if (this.throttled == false) {
            // If they've gone above the current queue max size, then
            // they are throtttled!
            if (last_size >= this.throttleThreshold) {
                if (this.throttle_time_enabled) this.throttle_time.start();
                if (can_change && this.allowDecreaseOnThrottle) {
                    if (trace.val) LOG.trace("throttleThreshold=>"+this.throttleThreshold);
                    synchronized (this.size) {
                        if (this.throttled == false) {
                            this.throttleThreshold = Math.max(this.autoMinSize, (this.throttleThreshold - this.autoDelta));
                            this.computeReleaseThreshold();
                        }
                        this.throttled = true;
                    } // SYNCH
                } else {
                    this.throttled = true;
                }
            }
            
            // Or if the queue is completely empty and we're allowe to increase
            // the max limit, then we'll go ahead and do that for them here
            else if (can_change && last_size == 0 && this.allowIncreaseOnZero) {
                if (trace.val) LOG.trace("throttleThreshold=>"+this.throttleThreshold);
                synchronized (this.size) {
                    this.throttleThreshold = Math.min(this.autoMaxSize, (this.throttleThreshold + this.autoDelta));
                    this.computeReleaseThreshold();
                } // SYNCH
            }
        }
        // If we're throttled and we've gone below our release
        // threshold, then we can go ahead and unthrottle them
        else if (last_size <= this.throttleRelease) {
            if (debug.val)
                LOG.debug(String.format("Unthrottling queue [size=%d > release=%d]",
                          last_size, this.throttleRelease));
            if (this.throttle_time_enabled) this.throttle_time.stopIfStarted();
            this.throttled = false;
        }
    }
    
    // ----------------------------------------------------------------------------
    // UTILITY METHODS
    // ----------------------------------------------------------------------------

    public void reset() {
        this.throttleThreshold = this.origThrottleThreshold;
        this.throttled = false;
        int new_size = this.queue.size();
        this.size.lazySet(new_size);
        this.checkThrottling(false, new_size);
    }
    
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
     * @param threshold
     */
    public void setThrottleThreshold(int threshold) {
        assert(threshold > 0);
        this.throttleThreshold = threshold;
        this.origThrottleThreshold = threshold;
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
        return (this.autoDelta);
    }
    /**
     * Set the delta that the queue will automatically increase the throttle
     * threshold if the size of this queue goes to zero. 
     * This will only matter if allow_increase is set to true.
     * @param delta
     */
    public void setThrottleThresholdAutoDelta(int delta) {
        this.autoDelta = delta;
    }
    
    /**
     * Set the min size of the throttling threshold. If this queue is allowed
     * to decrease the threshold whenever the queue is throttled, then this is
     * the minimum size that the threshold will be allowed to decreased to.
     * @param size
     */
    public void setThrottleThresholdMinSize(int size) {
        this.autoMinSize = size;
    }
    public int getThrottleThresholdMinSize() {
        return (this.autoMinSize);
    }
    
    /**
     * Set the max size of the throttling threshold. If this queue is allowed
     * to increase the threshold whenever the queue is empty, then this is
     * the maximum size that the threshold will be allowed to increase to.
     * @param size
     */
    public void setThrottleThresholdMaxSize(int size) {
        this.autoMaxSize = size;
    }
    public int getThrottleThresholdMaxSize() {
        return (this.autoMaxSize);
    }
    
    /**
     * If the allow_increase flag is set to true, then the
     * throttle threshold will be allowed to automatically grow
     * whenever the the size of the queue is zero.
     * @param allow
     */
    public void setAllowIncrease(boolean allow) {
        this.allowIncreaseOnZero = allow;
    }
    /**
     * If the allow_decrease flag is set to true, then the
     * throttle threshold will be allowed to automatically decrease
     * whenever the queue is throttled.
     * @param allow
     */
    public void setAllowDecrease(boolean allow) {
        this.allowDecreaseOnThrottle = allow;
    }
    
    protected final Queue<E> getQueue() {
        return (this.queue);
    }
    
    public void enableProfiling(boolean val) {
        this.throttle_time_enabled = val;
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
        if (ret) {
            int size = this.size.incrementAndGet();
            // If they had us force the element into the queue, then
            // we won't check to see whether to enable throttling
            if (force == false) {
                // Since we're adding an item, we know that the size
                // is unlikely to be zero.
                this.checkThrottling(this.allowDecreaseOnThrottle, size);
            }
        }
        return (ret);
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
    public boolean remove(Object o) {
        boolean ret = this.queue.remove(o);
        if (ret) {
            this.checkThrottling(this.allowIncreaseOnZero, this.size.decrementAndGet());
        }
        return (ret);
    }
    @Override
    public E poll() {
        E e = this.queue.poll();
        if (e != null) {
            this.checkThrottling(this.allowIncreaseOnZero, this.size.decrementAndGet());
        }
        return (e);
    }
    @Override
    public E remove() {
        E e = this.queue.remove();
        if (e != null) {
            this.checkThrottling(this.allowIncreaseOnZero, this.size.decrementAndGet());
        }
        return (e);
    }
    @Override
    public void clear() {
        if (this.throttle_time_enabled && this.throttled) this.throttle_time.stopIfStarted();
        this.throttled = false;
        this.size.set(0);
        this.queue.clear();
        this.checkThrottling(false, 0);
    }
    @Override
    public boolean removeAll(Collection<?> c) {
        boolean ret = this.queue.removeAll(c);
        if (ret) {
            int new_size = this.queue.size();
            this.size.lazySet(new_size);
            this.checkThrottling(this.allowIncreaseOnZero, new_size);
        }
        return (ret);
    }
    @Override
    public boolean retainAll(Collection<?> c) {
        boolean ret = this.queue.retainAll(c);
        if (ret) {
            int new_size = this.queue.size();
            this.size.lazySet(new_size);
            this.checkThrottling(this.allowIncreaseOnZero, new_size);
        }
        return (ret);
    }
    @Override
    public boolean addAll(Collection<? extends E> c) {
        boolean ret = this.queue.addAll(c);
        if (ret) {
            int new_size = this.queue.size();
            this.size.lazySet(new_size);
            this.checkThrottling(false, new_size);
        }
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
    /**
     * Lock-free approximation of whether the underlying queue is empty
     * This is not guaranteed to be correct.
     * @return
     */
    public boolean approximateIsEmpty() {
        return (this.size.get() == 0);
    }
    @Override
    public Iterator<E> iterator() {
        return (this.queue.iterator());
    }
    @Override
    public int size() {
        int new_size = this.queue.size();
        this.size.lazySet(new_size);
        return (new_size);
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
        return String.format("%s [max=%d / release=%d / delta=%d]",
                             this.queue.toString(),
                             this.throttleThreshold, this.throttleRelease, this.autoDelta);
    }
    
    public String debug() {
        Map<String, Object> m = new LinkedHashMap<String, Object>();
        m.put("Size", String.format("Actual=%d / Approx=%d", this.queue.size(), this.size.get()));
        m.put("Throttled", this.throttled);
        m.put("Threshold", this.throttleThreshold);
        m.put("Release", String.format("%d [%.1f%%]", this.throttleRelease, this.throttleReleaseFactor*100));
        m.put("Allow Decrease", this.allowIncreaseOnZero);
        m.put("Allow Increase", this.allowIncreaseOnZero);
        m.put("Increase Delta", this.autoDelta);
        m.put("Increase Min", this.autoMinSize);
        m.put("Increase Max", this.autoMaxSize);
        return (StringUtil.formatMaps(m));
    }
}
