package edu.brown.profilers;

import edu.brown.statistics.FastIntHistogram;

public class HStoreSiteProfiler extends AbstractProfiler {
    
    /**
     * How much time the VoltProcedureListener spent not processing
     * new incoming requests from clients. 
     */
    public final ProfileMeasurement network_idle_time = new ProfileMeasurement("IDLE");
    
    /**
     * How long it takes for the VoltProcedureListener to process each request
     */
    public final ProfileMeasurement network_processing_time = new ProfileMeasurement("PROCESSING");
 
    
    /**
     * The number of incoming transaction requests per partition 
     */
    public final FastIntHistogram network_incoming_partitions = new FastIntHistogram();
    
    @Override
    public void reset() {
        super.reset();
        network_incoming_partitions.clear();
    }
}
