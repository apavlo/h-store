package edu.mit.hstore.callbacks;

import java.util.concurrent.CountDownLatch;

import com.google.protobuf.RpcCallback;

public class BlockingCallback<T> implements RpcCallback<T> {

    private final CountDownLatch latch = new CountDownLatch(1);
    private T parameter;
    
    public BlockingCallback() {
        // Do nothing...
    }
    
    public void run(T parameter) {
        this.latch.countDown();
        this.parameter = parameter;
    }
    
    public void block() {
        try {
            this.latch.await();
        } catch (InterruptedException ex) {
            // Do nothing...
        }
    }
    
    public T getParameter() {
        return (this.parameter);
    }
    
}
