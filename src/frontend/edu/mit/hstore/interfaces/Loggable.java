package edu.mit.hstore.interfaces;

public interface Loggable {

    /**
     * Callback method used to notify the implementing object that
     * the logging configuration has changed.
     */
    public void updateLogging();
    
}
