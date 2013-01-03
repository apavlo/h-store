package edu.brown.profilers;

import java.util.concurrent.atomic.AtomicInteger;

public class BatchPlannerProfiler extends AbstractProfiler {

    public final AtomicInteger transactions = new AtomicInteger(0);
    public final AtomicInteger cached = new AtomicInteger(0);
    
    public final ProfileMeasurement plan_time = new ProfileMeasurement("BUILD_PLAN");
    public final ProfileMeasurement partest_time = new ProfileMeasurement("PARTITION_EST");
    public final ProfileMeasurement graph_time = new ProfileMeasurement("BUILD_GRAPH");
    public final ProfileMeasurement fragment_time = new ProfileMeasurement("BUILD_FRAGMENTS");
    
    @Override
    public void reset() {
        super.reset();
        this.transactions.set(0);
        this.cached.set(0);
    }
}
