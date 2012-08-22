package edu.brown.profilers;

public class PartitionExecutorProfiler extends AbstractProfiler {

    /**
     * Simple counter of the total number of transactions that the corresponding
     * PartitionExecutor has executed. This is only the txns that it invoked locally, 
     * not ones that it executed queries on their behalf from remote partitions.
     * Not guaranteed to be thread-safe.
     */
    public long numTransactions = 0;
    
    /**
     * How much time the PartitionExecutor was idle waiting for
     * work to do in its queue
     */
    public final ProfileMeasurement idle_queue_time = new ProfileMeasurement("IDLE_QUEUE");
    
    /**
     * How much time the PartitionExecutor was idle waiting for responses 
     * from queries on remote partitions
     */
    public final ProfileMeasurement idle_dtxn_time = new ProfileMeasurement("IDLE_DTXN");
    
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
    
    @Override
    public void reset() {
        super.reset();
        this.numTransactions = 0;
    }
}
