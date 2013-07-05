package edu.brown.hstore.estimators;

import java.util.List;

import edu.brown.catalog.special.CountedStatement;
import edu.brown.hstore.estimators.Estimate;
import edu.brown.markov.EstimationThresholds;
import edu.brown.utils.PartitionSet;

public class MockEstimate implements Estimate {
    private final long remaining;
    
    public MockEstimate(long remaining) {
        this.remaining = remaining;
    }
    @Override
    public boolean isInitialized() {
        return (true);
    }
    @Override
    public void finish() { }
    @Override
    public boolean isInitialEstimate() {
        return true;
    }
    @Override
    public int getBatchId() {
        return 0;
    }
    @Override
    public boolean isValid() {
        return (true);
    }
    @Override
    public PartitionSet getTouchedPartitions(EstimationThresholds t) {
        return null;
    }
    @Override
    public long getRemainingExecutionTime() {
        return (this.remaining);
    }
    @Override
    public boolean hasQueryEstimate(int partition) {
        return false;
    }
    @Override
    public List<CountedStatement> getQueryEstimate(int partition) {
        return null;
    }
//    @Override
//    public boolean isSinglePartitionProbabilitySet() {
//        return false;
//    }
    @Override
    public boolean isSinglePartitioned(EstimationThresholds t) {
        return false;
    }
//    @Override
//    public boolean isReadOnlyProbabilitySet(int partition) {
//        return false;
//    }
    @Override
    public boolean isReadOnlyPartition(EstimationThresholds t, int partition) {
        return false;
    }
    @Override
    public boolean isReadOnlyAllPartitions(EstimationThresholds t) {
        return false;
    }
//    @Override
//    public PartitionSet getReadOnlyPartitions(EstimationThresholds t) {
//        return null;
//    }
    @Override
    public boolean isWriteProbabilitySet(int partition) {
        return false;
    }
    @Override
    public boolean isWritePartition(EstimationThresholds t, int partition) {
        return false;
    }
    @Override
    public PartitionSet getWritePartitions(EstimationThresholds t) {
        return null;
    }
    @Override
    public boolean isDoneProbabilitySet(int partition) {
        return false;
    }
    @Override
    public boolean isDonePartition(EstimationThresholds t, int partition) {
        return false;
    }
    @Override
    public PartitionSet getDonePartitions(EstimationThresholds t) {
        return null;
    }
    @Override
    public boolean isAbortProbabilitySet() {
        return false;
    }
    @Override
    public boolean isAbortable(EstimationThresholds t) {
        return false;
    }
}