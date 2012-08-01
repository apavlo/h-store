package edu.brown.profilers;

public class PartitionExecutorProfiler extends AbstractProfiler {

    /**
     * How much time the PartitionExecutor was idle waiting for
     * work to do in its queue
     */
    public final ProfileMeasurement idle_time = new ProfileMeasurement("IDLE");
    
    /**
     * How much time it takes for this PartitionExecutor to 
     * execute a transaction
     */
    public final ProfileMeasurement exec_time = new ProfileMeasurement("EXEC");
    
    /**
     * How much time it takes for this PartitionExecutor spends sending
     * back ClientResponses over the network
     */
    public final ProfileMeasurement network_time = new ProfileMeasurement("NETWORK");
    
    /**
     * How much time did this PartitionExecutor spend on utility work
     */
    public final ProfileMeasurement util_time = new ProfileMeasurement("UTILITY");
}
