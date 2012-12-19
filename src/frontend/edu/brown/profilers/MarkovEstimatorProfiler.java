package edu.brown.profilers;

public class MarkovEstimatorProfiler extends AbstractProfiler {

    public final ProfileMeasurement time_start = new ProfileMeasurement("START_TXN");
    public final ProfileMeasurement time_update = new ProfileMeasurement("UPDATE_TXN");
    public final ProfileMeasurement time_finish = new ProfileMeasurement("FINISH_TXN");
    
    public final ProfileMeasurement time_consume = new ProfileMeasurement("CONSUME");
    public final ProfileMeasurement time_full_estimate = new ProfileMeasurement("FULL_ESTIMATE");
    public final ProfileMeasurement time_fast_estimate = new ProfileMeasurement("FAST_ESTIMATE");
    public final ProfileMeasurement time_cached_estimate = new ProfileMeasurement("CACHED_ESTIMATE");
    
}
