package edu.brown.profilers;

public class MarkovEstimatorProfiler extends AbstractProfiler {

    public final ProfileMeasurement time_start = new ConcurrentProfileMeasurement("START_TXN");
    public final ProfileMeasurement time_update = new ConcurrentProfileMeasurement("UPDATE_TXN");
    public final ProfileMeasurement time_finish = new ConcurrentProfileMeasurement("FINISH_TXN");
    
    public final ProfileMeasurement time_consume = new ConcurrentProfileMeasurement("CONSUME");
    public final ProfileMeasurement time_full_estimate = new ConcurrentProfileMeasurement("FULL_ESTIMATE");
    public final ProfileMeasurement time_fast_estimate = new ConcurrentProfileMeasurement("FAST_ESTIMATE");
    public final ProfileMeasurement time_cached_estimate = new ConcurrentProfileMeasurement("CACHED_ESTIMATE");
    
}
