package edu.brown.hstore;

import java.util.PriorityQueue;

import edu.brown.hstore.util.DeferredWork;

/**
 * There's probably no reason this needs to be its own class with overrides, except
 * this way we can experiment more easily for now.
 * @author ambell
 */
public class PartitionExecutorDeferredQueue extends PriorityQueue<DeferredWork> {
    
    private static final long serialVersionUID = 1L;
    
    public PartitionExecutorDeferredQueue() {
        super(100); // FIXME
    }
    
    @Override
    public DeferredWork poll() {
        return this.poll();
    }
}
