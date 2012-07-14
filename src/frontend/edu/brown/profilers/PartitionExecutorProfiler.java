package edu.brown.profilers;

public class PartitionExecutorProfiler {

    /**
     * How much time the PartitionExecutor was idle waiting for
     * work to do in its queue
     */
    public final ProfileMeasurement work_idle_time = new ProfileMeasurement("EE_IDLE");
    
    /**
     * How much time it takes for this PartitionExecutor to 
     * execute a transaction
     */
    public final ProfileMeasurement work_exec_time = new ProfileMeasurement("EE_EXEC");
    
    /**
     * How much time it takes for this PartitionExecutor spends sending
     * back ClientResponses over the network
     */
    public final ProfileMeasurement work_network_time = new ProfileMeasurement("EE_NETWORK");
    
    /**
     * How much time did this PartitionExecutor spend on utility work
     */
    public final ProfileMeasurement work_utility_time = new ProfileMeasurement("EE_UTILITY");
}
