package edu.brown.profilers;

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
    
}
