package edu.brown.hstore.callbacks;

import edu.brown.hstore.Hstoreservice.Status;
import edu.brown.pools.Poolable;

public interface TransactionCallback extends Poolable {

    // ----------------------------------------------------------------------------
    // ABORT METHODS
    // ----------------------------------------------------------------------------
    
    /**
     * Mark this callback as aborted with the given status.
     * @param status
     */
    public void abort(int partition, Status status);
    /**
     * Returns true if this callback has invoked the abortCallback() method
     */
    public boolean isAborted();
    
    // ----------------------------------------------------------------------------
    // CANCEL METHODS
    // ----------------------------------------------------------------------------
    
    /**
     * Mark this callback as canceled. No matter what happens in the future,
     * this callback will not invoke either the run or abort callbacks
     */
    public void cancel();
    /**
     * Returns true if this callback has been cancelled
     */
    public boolean isCanceled();
    
    
}
