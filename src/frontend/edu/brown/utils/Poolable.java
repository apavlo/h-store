package edu.brown.utils;

/**
 * An object that can be pooled
 * @author pavlo
 */
public interface Poolable {
    /**
     * Callback method to clean-up the internal state of this poolable object
     * This method should not return the object back to its corresponding pool
     */
    public void finish();
}
