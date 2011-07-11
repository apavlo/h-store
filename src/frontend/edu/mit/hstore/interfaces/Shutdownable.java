package edu.mit.hstore.interfaces;

public interface Shutdownable {

    enum State {
        INITIALIZED,
        STARTED,
        PREPARE_SHUTDOWN,
        SHUTDOWN,
    };
    
    public void prepareShutdown();
    public void shutdown();
    public boolean isShuttingDown();
    
}
