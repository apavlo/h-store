package edu.brown.profilers;

import edu.brown.statistics.FastIntHistogram;

public class HStoreSiteProfiler extends AbstractProfiler {
    
    /**
     * 
     */
    public final ProfileMeasurement network_processing = new ConcurrentProfileMeasurement("PROCESSING");
    
    /**
     * How much time the site spends with backup pressure blocking disabled
     */
    public final ProfileMeasurement network_backup_off = new ConcurrentProfileMeasurement("BACKUP_OFF");
    
    /**
     * How much time the site spends with backup pressure blocking enabled
     */
    public final ProfileMeasurement network_backup_on = new ConcurrentProfileMeasurement("BACKUP_ON");
    
    /**
     * How much time the VoltProcedureListener spent not processing
     * new incoming requests from clients. 
     */
    public final ProfileMeasurement network_idle = new ConcurrentProfileMeasurement("IDLE");
    
    /**
     * How long the clean-up thread spends to delete transaction handles
     */
    public final ProfileMeasurement cleanup = new ConcurrentProfileMeasurement("CLEAN_UP");
 
    /**
     * The number of incoming transaction requests per partition 
     */
    public final FastIntHistogram network_incoming_partitions = new FastIntHistogram();
    
    @Override
    public void reset() {
        super.reset();
        this.network_incoming_partitions.clear();
    }
}
