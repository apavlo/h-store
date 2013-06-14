package edu.brown.hstore.estimators.fixed;

import java.util.List;

import org.apache.commons.collections15.CollectionUtils;
import org.voltdb.CatalogContext;
import org.voltdb.catalog.Procedure;
import org.voltdb.catalog.Statement;
import org.voltdb.utils.EstTime;

import edu.brown.catalog.special.CountedStatement;
import edu.brown.hstore.Hstoreservice.Status;
import edu.brown.hstore.estimators.EstimatorState;
import edu.brown.hstore.estimators.Estimate;
import edu.brown.hstore.estimators.EstimatorUtil;
import edu.brown.hstore.estimators.TransactionEstimator;
import edu.brown.markov.EstimationThresholds;
import edu.brown.utils.PartitionEstimator;
import edu.brown.utils.PartitionSet;
import edu.brown.utils.ProjectType;

public abstract class AbstractFixedEstimator extends TransactionEstimator {
    
    protected static final PartitionSet EMPTY_PARTITION_SET = new PartitionSet();
    
    public AbstractFixedEstimator(PartitionEstimator p_estimator) {
        super(p_estimator);
    }

    @SuppressWarnings("unchecked")
    @Override
    public final Estimate executeQueries(EstimatorState state, Statement[] catalog_stmts, PartitionSet[] partitions) {
        return (state.getInitialEstimate());
    }
    
    @Override
    protected final void completeTransaction(EstimatorState state, Status status) {
        // Nothing to do
    }
    
    @Override
    public final void destroyEstimatorState(EstimatorState state) {
        // Nothing to do...
    }
    
    @SuppressWarnings("unchecked")
    public static <T extends AbstractFixedEstimator> T factory(PartitionEstimator p_estimator, CatalogContext catalogContext) {
        AbstractFixedEstimator estimator = null;
        ProjectType ptype = ProjectType.get(catalogContext.database.getProject());
        switch (ptype) {
            case TPCC:
                estimator = new FixedTPCCEstimator(p_estimator);
                break;
            case TM1:
                estimator = new FixedTM1Estimator(p_estimator);
                break;
            case SEATS:
                estimator = new FixedSEATSEstimator(p_estimator);
                break;
            case VOTER:
                estimator = new FixedVoterEstimator(p_estimator);
                break;
            default:
                estimator = new DefaultFixedEstimator(p_estimator);
        } // SWITCH
        return ((T)estimator);
    }
    
    protected static class DefaultFixedEstimator extends AbstractFixedEstimator {
        public DefaultFixedEstimator(PartitionEstimator p_estimator) {
            super(p_estimator);
        }
        @SuppressWarnings("unchecked")
        @Override
        protected <T extends EstimatorState> T startTransactionImpl(Long txn_id, int base_partition, Procedure catalog_proc, Object[] args) {
            FixedEstimatorState ret = new FixedEstimatorState(this.catalogContext, txn_id, base_partition);
            PartitionSet partitions = this.catalogContext.getPartitionSetSingleton(base_partition);
            PartitionSet readonly = EMPTY_PARTITION_SET;
            ret.createInitialEstimate(partitions, readonly, EMPTY_PARTITION_SET);
            return ((T)ret);
        }
    } // CLASS
    
    /**
     * Fixed Estimator State 
     */
    protected static class FixedEstimatorState extends EstimatorState {

        protected FixedEstimatorState(CatalogContext catalogContext, Long txn_id, int base_partition) {
            super(catalogContext);
            this.init(txn_id, base_partition, EstTime.currentTimeMillis());
        }
        
        protected FixedEstimation createInitialEstimate(PartitionSet partitions,
                                                        PartitionSet readonly,
                                                        PartitionSet finished) {
            FixedEstimation next = new FixedEstimation(partitions, readonly, finished);
            this.addInitialEstimate(next);
            return (next);
        }
        
    } // CLASS

    /**
     * Fixed Estimator Estimate
     * @author pavlo
     */
    protected static class FixedEstimation implements Estimate {
        protected final PartitionSet partitions;
        protected final PartitionSet readonly;
        protected final PartitionSet finished;

        private FixedEstimation(PartitionSet partitions, PartitionSet readonly, PartitionSet finished) {
            this.partitions = partitions;
            this.readonly = readonly;
            this.finished = finished;
        }

        @Override
        public boolean isInitialized() {
            return (this.partitions != null);
        }

        @Override
        public void finish() {
            // Nothing to do...
        }
        
        @Override
        public boolean isInitialEstimate() {
            return (true);
        }
        
        @Override
        public int getBatchId() {
            return (EstimatorUtil.INITIAL_ESTIMATE_BATCH);
        }

        @Override
        public boolean isValid() {
            return (this.partitions.isEmpty() == false);
        }

        @Override
        public PartitionSet getTouchedPartitions(EstimationThresholds t) {
            return (this.partitions);
        }
        
        @Override
        public long getRemainingExecutionTime() {
            return Long.MAX_VALUE;
        }

        // ----------------------------------------------------------------------------
        // QUERIES
        // ----------------------------------------------------------------------------
        
        @Override
        public boolean hasQueryEstimate(int partition) {
            return false;
        }
        
        @Override
        public List<CountedStatement> getQueryEstimate(int partition) {
            // TODO Auto-generated method stub
            return null;
        }
        
        // ----------------------------------------------------------------------------
        // SINGLE-PARTITION PROBABILITY
        // ----------------------------------------------------------------------------
        
//        @Override
//        public boolean isSinglePartitionProbabilitySet() {
//            return (this.isValid());
//        }

        @Override
        public boolean isSinglePartitioned(EstimationThresholds t) {
            return (this.partitions.size() == 1);
        }

        // ----------------------------------------------------------------------------
        // READ-ONLY
        // ----------------------------------------------------------------------------
//        @Override
//        public boolean isReadOnlyProbabilitySet(int partition) {
//            return (this.isValid());
//        }
        @Override
        public boolean isReadOnlyAllPartitions(EstimationThresholds t) {
            return (this.partitions.size() == this.readonly.size());
        }
        @Override
        public boolean isReadOnlyPartition(EstimationThresholds t, int partition) {
            return (this.readonly.contains(Integer.valueOf(partition)));
        }
//        @Override
//        public PartitionSet getReadOnlyPartitions(EstimationThresholds t) {
//            return (this.readonly);
//        }

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
        public boolean isDoneProbabilitySet(int partition) {
            return (this.isValid());
        }
        @Override
        public PartitionSet getDonePartitions(EstimationThresholds t) {
            return (this.finished);
        }
        @Override
        public boolean isDonePartition(EstimationThresholds t, int partition) {
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
