package edu.brown.utils;

/**
 * An object that can be pooled
 * @author pavlo
 */
public interface Poolable {
    /**
     * Callback method to clean-up the internal state of this poolable object
     * This should only be invoked inside of ObjectPoolFactory.passivateObject()
     * Hence, an implementing method should not return the object back to its corresponding pool
     */
    public void finish();
}
