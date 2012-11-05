package edu.brown.hstore.estimators.fixed;

import org.voltdb.catalog.Procedure;
import org.voltdb.catalog.Statement;

import edu.brown.hstore.Hstoreservice.Status;
import edu.brown.hstore.estimators.EstimatorState;
import edu.brown.hstore.estimators.Estimate;
import edu.brown.utils.PartitionEstimator;
import edu.brown.utils.PartitionSet;

/**
 * TM1 Benchmark Fixed Estimator
 * @author pavlo
 */
public class TM1Estimator extends FixedEstimator {

    public TM1Estimator(PartitionEstimator p_estimator) {
        super(p_estimator);
    }
    
    @Override
    public EstimatorState startTransactionImpl(Long txn_id, int base_partition, Procedure catalog_proc, Object[] args) {
        String procName = catalog_proc.getName();
        
        // TODO
        if (procName.equalsIgnoreCase("UpdateLocation")) {
            
        }
        
        return (null);
    }

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
