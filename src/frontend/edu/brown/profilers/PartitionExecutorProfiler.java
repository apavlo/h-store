package edu.brown.profilers;

public class PartitionExecutorProfiler extends AbstractProfiler {

    /**
     * Simple counter of the total number of transactions that the corresponding
     * PartitionExecutor has executed. This is only the txns that it invoked locally, 
     * not ones that it executed queries on their behalf from remote partitions.
     * Not guaranteed to be thread-safe.
     */
    public long numTransactions = 0;
    
    // ----------------------------------------------------------------------------
    // GLOBAL MEASUREMENTS
    // ----------------------------------------------------------------------------
    
    /**
     * How much did the PartitionExecutor spend doing actual execution work, as opposed
     * to blocked on its PartitionMessageQueue waiting for something to do. 
     */
    public final ProfileMeasurement exec_time = new ProfileMeasurement("EXEC");
    
    /**
     * How much time it takes for this PartitionExecutor to 
     * execute a transaction
     */
    public final ProfileMeasurement txn_time = new ProfileMeasurement("TXN");
    
    /**
     * How much time the PartitionExecutor idle waiting for something in its
     * PartitionMessageQueue.
     */
    public final ProfileMeasurement idle_time = new ProfileMeasurement("IDLE");
    
    /**
     * How much time the PartitionExecutor spends polling its PartitionLockQueue
     */
    public final ProfileMeasurement poll_time = new ProfileMeasurement("POLL");
    
    /**
     * How much time it takes for this PartitionExecutor spends sending
     * back ClientResponses over the network
     */
    public final ProfileMeasurement network_time = new ProfileMeasurement("NETWORK");
    
    /**
     * How much time did this PartitionExecutor spend on utility work
     */
    public final ProfileMeasurement util_time = new ProfileMeasurement("UTILITY");
    
    /**
     * How much time did this PartitionExecutor spend in the spec exec conflict checker
     */
    public final ProfileMeasurement conflicts_time = new ProfileMeasurement("CONFLICTS");
    
    // ----------------------------------------------------------------------------
    // FINE-GRAINED IDLE MEASUREMENTS
    // ----------------------------------------------------------------------------
    
    /**
     * How much time the PartitionExecutor was idle waiting for responses 
     * from queries on remote partitions. (SP1)
     */
    public final ProfileMeasurement idle_dtxn_query_time = new ConcurrentProfileMeasurement("IDLE_DTXN_QUERY");
    
    /**
     * How much time the PartitionExecutor was idle waiting for
     * response of distributed transaction on remote partitions. (SP2.remote)
     */
    public final ProfileMeasurement idle_waiting_dtxn_time = new ConcurrentProfileMeasurement("IDLE_WAITING_DTXN");
    
    /**
     * How much time the PartitionExecutor was idle waiting for
     * response of distributed transaction on remote partitions. (SP2.remote)
     */
    public final ProfileMeasurement idle_queue_dtxn_time = new ProfileMeasurement("IDLE_QUEUE_DTXN");
    
    /**
     * How much time the local PartitionExecutor was idle waiting for prepare 
     * responses from remote partitions. (SP3.local) 
     */
    public final ProfileMeasurement idle_2pc_local_time = new ConcurrentProfileMeasurement("IDLE_TWO_PHASE_LOCAL");
    
    /**
     * How much time the remote PartitionExecutor was idle waiting for commit/abort
     * messages from base partition. (SP3.remote)
     */
    public final ProfileMeasurement idle_2pc_remote_time = new ConcurrentProfileMeasurement("IDLE_TWO_PHASE_REMOTE");
    
    @Override
    public void reset() {
        super.reset();
        this.numTransactions = 0;
    }
}
