package edu.brown.profilers;

public class BatchPlannerProfiler extends AbstractProfiler {

    public final ProfileMeasurement time_plan = new ProfileMeasurement("BuildPlan");
    public final ProfileMeasurement time_partitionEstimator = new ProfileMeasurement("PartitionEstimator");
    public final ProfileMeasurement time_planGraph = new ProfileMeasurement("BuildGraph");
    public final ProfileMeasurement time_partitionFragments = new ProfileMeasurement("BuildFragments");
}
