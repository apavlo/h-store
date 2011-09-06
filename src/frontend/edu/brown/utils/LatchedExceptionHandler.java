package edu.brown.utils;

import java.util.concurrent.CountDownLatch;

public class LatchedExceptionHandler implements Thread.UncaughtExceptionHandler {

    private final CountDownLatch latch;
    private Throwable last_error;
    
    public LatchedExceptionHandler(CountDownLatch latch) {
        assert(latch.getCount() > 0);
        this.latch = latch;
    }
    
    @Override
    public void uncaughtException(Thread t, Throwable e) {
        this.last_error = e;
        while (this.latch.getCount() > 0) this.latch.countDown();
    }
    
    public boolean hasError() {
        return (this.last_error != null);
    }
    
    public Throwable getLastError() {
        return (this.last_error);
    }
    
}
