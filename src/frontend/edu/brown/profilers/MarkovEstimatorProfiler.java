package edu.brown.profilers;

public class MarkovEstimatorProfiler extends AbstractProfiler {

    public final ProfileMeasurement time_full_estimate = new ProfileMeasurement("FULL_ESTIMATE");
    public final ProfileMeasurement time_fast_estimate = new ProfileMeasurement("FAST_ESTIMATE");
    
}
