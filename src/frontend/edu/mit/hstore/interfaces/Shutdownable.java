package edu.mit.hstore.interfaces;

public interface Shutdownable {

    enum ShutdownState {
        INITIALIZED,
        STARTED,
        PREPARE_SHUTDOWN,
        SHUTDOWN,
    };
    
    public void prepareShutdown(boolean error);
    public void shutdown();
    public boolean isShuttingDown();
    
}
