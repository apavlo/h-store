package edu.brown.profilers;

import java.util.concurrent.atomic.AtomicInteger;

public class BatchPlannerProfiler extends AbstractProfiler {

    public final AtomicInteger transactions = new AtomicInteger(0);
    
    public final ProfileMeasurement time_plan = new ProfileMeasurement("BuildPlan");
    public final ProfileMeasurement time_partitionEstimator = new ProfileMeasurement("PartitionEstimator");
    public final ProfileMeasurement time_planGraph = new ProfileMeasurement("BuildGraph");
    public final ProfileMeasurement time_partitionFragments = new ProfileMeasurement("BuildFragments");
    
    @Override
    public void reset() {
        super.reset();
        this.transactions.set(0);
    }
}
