package edu.brown.hstore.estimators.fixed;

import org.voltdb.catalog.Procedure;
import org.voltdb.catalog.Statement;

import edu.brown.hstore.Hstoreservice.Status;
import edu.brown.hstore.estimators.Estimate;
import edu.brown.hstore.estimators.EstimatorState;
import edu.brown.utils.PartitionEstimator;
import edu.brown.utils.PartitionSet;

/**
 * Voter Benchmark Fixed Estimator
 * @author pavlo
 */
public class FixedVoterEstimator extends AbstractFixedEstimator {

    public FixedVoterEstimator(PartitionEstimator p_estimator) {
        super(p_estimator);
    }
    
    @SuppressWarnings("unchecked")
    @Override
    public EstimatorState startTransactionImpl(Long txn_id, int base_partition, Procedure catalog_proc, Object[] args) {
        String procName = catalog_proc.getName();
        FixedEstimatorState ret = new FixedEstimatorState(this.catalogContext, txn_id, base_partition);
        PartitionSet partitions = null;
        PartitionSet readonly = EMPTY_PARTITION_SET;
        if (procName.equalsIgnoreCase("vote")) {
            partitions = this.catalogContext.getPartitionSetSingleton(base_partition);
        } else {
            partitions = this.catalogContext.getAllPartitionIds();
        }
        ret.createInitialEstimate(partitions, readonly, EMPTY_PARTITION_SET);
        return (ret);
    }

    @SuppressWarnings("unchecked")
    @Override
    public Estimate executeQueries(EstimatorState state, Statement[] catalog_stmts, PartitionSet[] partitions) {
        return (state.getInitialEstimate());
    }
    
    @Override
    protected void completeTransaction(EstimatorState state, Status status) {
        // Nothing to do
    }

    @Override
    public void updateLogging() {
        // TODO Auto-generated method stub
        
    }
}
