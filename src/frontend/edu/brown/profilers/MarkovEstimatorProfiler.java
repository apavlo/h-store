package edu.brown.profilers;

public class MarkovEstimatorProfiler extends AbstractProfiler {

    public final ProfileMeasurement start_time = new ConcurrentProfileMeasurement("START_TXN");
    public final ProfileMeasurement update_time = new ConcurrentProfileMeasurement("UPDATE_TXN");
    public final ProfileMeasurement finish_time = new ConcurrentProfileMeasurement("FINISH_TXN");
    
    public final ProfileMeasurement consume_time = new ConcurrentProfileMeasurement("CONSUME");
    public final ProfileMeasurement fullest_time = new ConcurrentProfileMeasurement("FULL_ESTIMATE");
    public final ProfileMeasurement fastest_time = new ConcurrentProfileMeasurement("FAST_ESTIMATE");
    public final ProfileMeasurement cachedest_time = new ConcurrentProfileMeasurement("CACHED_ESTIMATE");
    
}
