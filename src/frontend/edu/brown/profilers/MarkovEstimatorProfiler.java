package edu.brown.profilers;

public class MarkovEstimatorProfiler extends AbstractProfiler {

    public final ProfileMeasurement time_full_estimate = new ProfileMeasurement("FULL_ESTIMATE");
    public final ProfileMeasurement time_fast_estimate = new ProfileMeasurement("FAST_ESTIMATE");
    public final ProfileMeasurement time_cached_estimate = new ProfileMeasurement("CACHED_ESTIMATE");
    
}
