package edu.brown.utils;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingDeque;

public abstract class Consumer<T> implements Runnable {

    private final LinkedBlockingDeque<T> queue = new LinkedBlockingDeque<T>();
    private final CountDownLatch producer_latch = new CountDownLatch(1);
    private Producer<?, T> producer;
    private Thread self;
    private int counter = 0;
     
    @Override
    public void run() {
        this.self = Thread.currentThread();
        this.counter = 0;
        
        T t = null;
        while (true) {
            try {
                // Wait until we have our producer
                if (this.producer_latch.getCount() > 0) this.producer_latch.await();

                // If the producer has queued everything, poll it right away
                // That way we'll know when our queue is empty for good
                if (this.producer.hasQueuedAll()) {
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
    
    public void setProducer(Producer<?, T> producer) {
        this.producer = producer;
        this.producer_latch.countDown();
    }
    
    public void queue(T t) {
        this.queue.add(t);
    }

    public void interrupt() {
        if (this.self != null) {
            this.self.interrupt();
        }
    }
}
