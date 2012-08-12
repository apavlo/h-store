package edu.brown.hstore.estimators;

import org.voltdb.CatalogContext;

import edu.brown.markov.EstimationThresholds;
import edu.brown.utils.PartitionEstimator;
import edu.brown.utils.PartitionSet;
import edu.brown.utils.ProjectType;

public abstract class FixedEstimator extends AbstractEstimator {
    
    public FixedEstimator(PartitionEstimator p_estimator) {
        super(p_estimator);
    }

    public static FixedEstimator getFixedEstimator(PartitionEstimator p_estimator, CatalogContext catalogContext){
        FixedEstimator estimator = null;
        ProjectType ptype = ProjectType.get(catalogContext.database.getProject());
        switch (ptype) {
            case TPCC:
                estimator = new TPCCEstimator(p_estimator);
                break;
            case TM1:
                estimator = new TM1Estimator(p_estimator);
                break;
            case SEATS:
                estimator = new SEATSEstimator(p_estimator);
                break;
            default:
                estimator = null;
        } // SWITCH
        return (estimator);
    }
    
    protected static class FixedEstimatorState extends EstimatorState {

        protected FixedEstimatorState(int num_partitions) {
            super(num_partitions);
        }

        @Override
        public Estimation getInitialEstimate() {
            // TODO Auto-generated method stub
            return null;
        }

        @Override
        public Estimation getLastEstimate() {
            // TODO Auto-generated method stub
            return null;
        }
    } // CLASS
    
    protected static class FixedEstimation implements Estimation {

        @Override
        public boolean isValid() {
            // TODO Auto-generated method stub
            return false;
        }

        @Override
        public PartitionSet getTouchedPartitions(EstimationThresholds t) {
            // TODO Auto-generated method stub
            return null;
        }

        @Override
        public void addSingleSitedProbability(float probability) {
            // TODO Auto-generated method stub
            
        }

        @Override
        public void setSingleSitedProbability(float probability) {
            // TODO Auto-generated method stub
            
        }

        @Override
        public float getSingleSitedProbability() {
            // TODO Auto-generated method stub
            return 0;
        }

        @Override
        public boolean isSingleSitedProbabilitySet() {
            // TODO Auto-generated method stub
            return false;
        }

        @Override
        public boolean isSinglePartition(EstimationThresholds t) {
            // TODO Auto-generated method stub
            return false;
        }

        @Override
        public void addReadOnlyProbability(int partition, float probability) {
            // TODO Auto-generated method stub
            
        }

        @Override
        public void setReadOnlyProbability(int partition, float probability) {
            // TODO Auto-generated method stub
            
        }

        @Override
        public float getReadOnlyProbability(int partition) {
            // TODO Auto-generated method stub
            return 0;
        }

        @Override
        public boolean isReadOnlyProbabilitySet(int partition) {
            // TODO Auto-generated method stub
            return false;
        }

        @Override
        public boolean isReadOnlyAllPartitions(EstimationThresholds t) {
            // TODO Auto-generated method stub
            return false;
        }

        @Override
        public boolean isReadOnlyPartition(EstimationThresholds t, int partition) {
            // TODO Auto-generated method stub
            return false;
        }

        @Override
        public PartitionSet getReadOnlyPartitions(EstimationThresholds t) {
            // TODO Auto-generated method stub
            return null;
        }

        @Override
        public void addWriteProbability(int partition, float probability) {
            // TODO Auto-generated method stub
            
        }

        @Override
        public void setWriteProbability(int partition, float probability) {
            // TODO Auto-generated method stub
            
        }

        @Override
        public float getWriteProbability(int partition) {
            // TODO Auto-generated method stub
            return 0;
        }

        @Override
        public boolean isWriteProbabilitySet(int partition) {
            // TODO Auto-generated method stub
            return false;
        }

        @Override
        public boolean isWritePartition(EstimationThresholds t, int partition) {
            // TODO Auto-generated method stub
            return false;
        }

        @Override
        public PartitionSet getWritePartitions(EstimationThresholds t) {
            // TODO Auto-generated method stub
            return null;
        }

        @Override
        public void addDoneProbability(int partition, float probability) {
            // TODO Auto-generated method stub
            
        }

        @Override
        public void setDoneProbability(int partition, float probability) {
            // TODO Auto-generated method stub
            
        }

        @Override
        public float getDoneProbability(int partition) {
            // TODO Auto-generated method stub
            return 0;
        }

        @Override
        public boolean isDoneProbabilitySet(int partition) {
            // TODO Auto-generated method stub
            return false;
        }

        @Override
        public PartitionSet getFinishedPartitions(EstimationThresholds t) {
            // TODO Auto-generated method stub
            return null;
        }

        @Override
        public void addAbortProbability(float probability) {
            // TODO Auto-generated method stub
            
        }

        @Override
        public void setAbortProbability(float probability) {
            // TODO Auto-generated method stub
            
        }

        @Override
        public float getAbortProbability() {
            // TODO Auto-generated method stub
            return 0;
        }

        @Override
        public boolean isAbortProbabilitySet() {
            // TODO Auto-generated method stub
            return false;
        }

        @Override
        public boolean isAbortable(EstimationThresholds t) {
            // TODO Auto-generated method stub
            return false;
        }

        @Override
        public boolean isFinishedPartition(EstimationThresholds t, int partition) {
            // TODO Auto-generated method stub
            return false;
        }
        
    }
    
}
