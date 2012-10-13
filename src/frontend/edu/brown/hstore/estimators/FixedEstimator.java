package edu.brown.hstore.estimators;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.collections15.CollectionUtils;
import org.voltdb.CatalogContext;
import org.voltdb.utils.EstTime;

import edu.brown.markov.EstimationThresholds;
import edu.brown.utils.CollectionUtil;
import edu.brown.utils.PartitionEstimator;
import edu.brown.utils.PartitionSet;
import edu.brown.utils.ProjectType;

public abstract class FixedEstimator extends TransactionEstimator {
    
    protected static final PartitionSet EMPTY_PARTITION_SET = new PartitionSet();
    
    public FixedEstimator(PartitionEstimator p_estimator) {
        super(p_estimator);
    }

    @SuppressWarnings("unchecked")
    public static <T extends FixedEstimator> T factory(PartitionEstimator p_estimator, CatalogContext catalogContext){
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
        return ((T)estimator);
    }
    
    /**
     * Fixed Estimator State 
     */
    protected static class FixedEstimatorState extends EstimatorState {
        private final List<FixedEstimation> estimates = new ArrayList<FixedEstimation>();
        
        protected FixedEstimatorState(Long txn_id, int num_partitions, int base_partition) {
            super(num_partitions);
            this.init(txn_id, base_partition, EstTime.currentTimeMillis());
        }
        
        protected FixedEstimation createNextEstimate(PartitionSet partitions,
                                                     PartitionSet readonly,
                                                     PartitionSet finished) {
            FixedEstimation next = new FixedEstimation(partitions, readonly, finished);
            this.estimates.add(next);
            return (next);
        }

        @Override
        public TransactionEstimate getInitialEstimate() {
            return CollectionUtil.first(this.estimates);
        }

        @Override
        public TransactionEstimate getLastEstimate() {
            return CollectionUtil.last(this.estimates);
        }
    } // CLASS

    /**
     * Fixed Estimator Estimate
     * @author pavlo
     */
    protected static class FixedEstimation implements TransactionEstimate {
        protected final PartitionSet partitions;
        protected final PartitionSet readonly;
        protected final PartitionSet finished;

        private FixedEstimation(PartitionSet partitions, PartitionSet readonly, PartitionSet finished) {
            this.partitions = partitions;
            this.readonly = readonly;
            this.finished = finished;
        }
        
        @Override
        public boolean isValid() {
            return (this.partitions.isEmpty() == false);
        }

        @Override
        public PartitionSet getTouchedPartitions(EstimationThresholds t) {
            return (this.partitions);
        }

        // ----------------------------------------------------------------------------
        // QUERIES
        // ----------------------------------------------------------------------------
        
        @Override
        public boolean hasQueryList() {
            return false;
        }
        
        // ----------------------------------------------------------------------------
        // SINGLE-PARTITION PROBABILITY
        // ----------------------------------------------------------------------------
        
        @Override
        public boolean isSinglePartitionProbabilitySet() {
            return (this.isValid());
        }

        @Override
        public boolean isSinglePartitioned(EstimationThresholds t) {
            return (this.partitions.size() == 1);
        }

        // ----------------------------------------------------------------------------
        // READ-ONLY
        // ----------------------------------------------------------------------------
        @Override
        public boolean isReadOnlyProbabilitySet(int partition) {
            return (this.isValid());
        }
        @Override
        public boolean isReadOnlyAllPartitions(EstimationThresholds t) {
            return (this.partitions.size() == this.readonly.size());
        }
        @Override
        public boolean isReadOnlyPartition(EstimationThresholds t, int partition) {
            return (this.readonly.contains(Integer.valueOf(partition)));
        }
        @Override
        public PartitionSet getReadOnlyPartitions(EstimationThresholds t) {
            return (this.readonly);
        }

        // ----------------------------------------------------------------------------
        // WRITE
        // ----------------------------------------------------------------------------
        @Override
        public boolean isWriteProbabilitySet(int partition) {
            return (this.isValid());
        }
        @Override
        public boolean isWritePartition(EstimationThresholds t, int partition) {
            return (this.isReadOnlyPartition(t, partition) == false);
        }
        @Override
        public PartitionSet getWritePartitions(EstimationThresholds t) {
            return new PartitionSet(CollectionUtils.subtract(this.partitions, this.readonly));
        }
        
        // ----------------------------------------------------------------------------
        // FINISH
        // ----------------------------------------------------------------------------
        @Override
        public boolean isFinishProbabilitySet(int partition) {
            return (this.isValid());
        }
        @Override
        public PartitionSet getFinishPartitions(EstimationThresholds t) {
            return (this.finished);
        }
        @Override
        public boolean isFinishPartition(EstimationThresholds t, int partition) {
            return (this.finished.contains(Integer.valueOf(partition)));
        }
        
        // ----------------------------------------------------------------------------
        // ABORT
        // ----------------------------------------------------------------------------
        @Override
        public boolean isAbortProbabilitySet() {
            return (true);
        }
        @Override
        public boolean isAbortable(EstimationThresholds t) {
            return (true);
        }
    } // CLASS
}
