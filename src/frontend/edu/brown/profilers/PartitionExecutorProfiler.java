package edu.brown.profilers;

import edu.brown.statistics.Histogram;
import edu.brown.statistics.ObjectHistogram;

public class PartitionExecutorProfiler extends AbstractProfiler {

    /**
     * Simple counter of the total number of transactions that the corresponding
     * PartitionExecutor has executed. This is only the txns that it invoked locally, 
     * not ones that it executed queries on their behalf from remote partitions.
     * Not guaranteed to be thread-safe.
     */
    public long numTransactions = 0;
    
    /**
     * Counter for the number of messages processed at this partition
     */
    public Histogram<String> numMessages = new ObjectHistogram<String>();
    
    /**
     * The timestamp of when this PartitionExecutor came on-line (in ms)
     */
    public long start_time;
    
    // ----------------------------------------------------------------------------
    // GLOBAL MEASUREMENTS
    // ----------------------------------------------------------------------------
    
    /**
     * How much did the PartitionExecutor spend doing actual execution work, as opposed
     * to blocked on its PartitionMessageQueue waiting for something to do. 
     */
    public final ProfileMeasurement exec_time = new ProfileMeasurement("EXEC");
    
    /**
     * How much did the PartitionExecutor spend executing speculative txns
     */
    public final ProfileMeasurement specexec_time = new ProfileMeasurement("SPECEXEC");
    
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
    // FINE-GRAINED STALL POINT MEASUREMENTS
    // ----------------------------------------------------------------------------
    
    /**
     * STALL POINT 1 - LOCAL
     * How much time the PartitionExecutor was idle waiting for responses 
     * from queries on remote partitions.
     */
    public final ProfileMeasurement sp1_time = new ProfileMeasurement("SP1");

    /**
     * STALL POINT 2 - REMOTE
     * How much time the PartitionExecutor was idle waiting for the first 
     * WorkResult response of distributed transaction on remote partitions.
     */
    public final ProfileMeasurement sp2_time = new ProfileMeasurement("SP2");
    
    /**
     * STALL POINT 3 - LOCAL
     * How much time the local PartitionExecutor was idle waiting for 2PC:PREPARE 
     * responses from remote partitions. (SP3.local) 
     */
    public final ProfileMeasurement sp3_local_time = new ProfileMeasurement("SP3_LOCAL");
    
    /**
     * STALL POINT 3 - REMOTE
     * How much time the remote PartitionExecutor was idle waiting for commit/abort
     * messages from base partition.
     */
    public final ProfileMeasurement sp3_remote_time = new ProfileMeasurement("SP3_REMOTE");
    
    @Override
    public void reset() {
        super.reset();
        this.start_time = System.currentTimeMillis();
        this.numTransactions = 0;
    }
}
