package edu.brown.profilers;

public class TransactionQueueManagerProfiler extends AbstractProfiler {

    public final ProfileMeasurement idle = new ProfileMeasurement("IDLE");
    
    public final ProfileMeasurement lock_queue = new ProfileMeasurement("LOCK_QUEUE");
    
    public final ProfileMeasurement init_queue = new ProfileMeasurement("INIT_QUEUE");
    
    public final ProfileMeasurement block_queue = new ProfileMeasurement("BLOCK_QUEUE");
    
    public final ProfileMeasurement restart_queue = new ProfileMeasurement("RESTART_QUEUE");
}
