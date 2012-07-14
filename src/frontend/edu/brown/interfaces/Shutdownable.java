package edu.brown.interfaces;

public interface Shutdownable {

    public enum ShutdownState {
        INITIALIZED,
        STARTED,
        PREPARE_SHUTDOWN,
        SHUTDOWN,
    };
    
    public void prepareShutdown(boolean error);
    public void shutdown();
    public boolean isShuttingDown();
    
}
