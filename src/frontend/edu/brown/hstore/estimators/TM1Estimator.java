package edu.brown.hstore.estimators;

import org.voltdb.catalog.Procedure;

import edu.brown.hstore.HStoreSite;
import edu.brown.utils.PartitionSet;

public class TM1Estimator extends AbstractEstimator {

    public TM1Estimator(HStoreSite hstore_site) {
        super(hstore_site);
    }
    
    @Override
    protected PartitionSet initializeTransactionImpl(Procedure catalog_proc, Object[] args, Object[] mangled) {
        String procName = catalog_proc.getName();
        PartitionSet ret = null;
        
        if (procName.equalsIgnoreCase("UpdateLocation")) {
            
        }
        
        return (ret);
    }

}
