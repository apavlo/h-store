package edu.brown.utils;

import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicBoolean;

public abstract class Consumer<T> implements Runnable {

    private final LinkedBlockingDeque<T> queue = new LinkedBlockingDeque<T>();
    private Semaphore start = new Semaphore(0);
    private final AtomicBoolean stopWhenEmpty = new AtomicBoolean(false);
    private Thread self;
    private int counter = 0;
     
    public Consumer() {
        // Nothing...
    }
    
    @Override
    public void run() {
        this.self = Thread.currentThread();
        this.counter = 0;
        
        // Wait until we have our producer
        this.start.acquireUninterruptibly();
        
        T t = null;
        while (true) {
            try {
                // If the producer has queued everything, poll it right away
                // That way we'll know when our queue is empty for good
                if (this.stopWhenEmpty.get()) {
                    t = this.queue.poll();
                }
                // Otherwise block until something gets added
                else {
                    t = this.queue.take();
                }
                
                // If the next item is null, then we want to stop right away
                if (t == null) break;
            
                this.process(t);
                this.counter++;
            } catch (InterruptedException ex) {
                // Ignore
            }
        } // WHILE
    }
    
    public abstract void process(T t);
    
    public int getProcessedCounter() {
        return (this.counter);
    }
    
    public void queue(T t) {
        this.queue.add(t);
    }

    public synchronized final void reset() {
        this.self = null;
        this.queue.clear();
        this.stopWhenEmpty.set(false);
        this.start.drainPermits();
    }
    
    public final void start() {
        this.start.release();
    }
    
    public final void stopWhenEmpty() {
        this.stopWhenEmpty.set(true);
        if (this.self != null) {
            this.self.interrupt();
        }
    }
}
