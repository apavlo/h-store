package edu.mit.hstore.estimators;

import java.util.Collection;

import org.voltdb.catalog.Procedure;

import edu.mit.hstore.HStoreSite;

public class TM1Estimator extends AbstractEstimator {

    public TM1Estimator(HStoreSite hstore_site) {
        super(hstore_site);
    }
    
    @Override
    protected Collection<Integer> initializeTransactionImpl(Procedure catalog_proc, Object[] args, Object[] mangled) {
        String procName = catalog_proc.getName();
        Collection<Integer> ret = null;
        
        if (procName.equalsIgnoreCase("UpdateLocation")) {
            
        }
        
        return (ret);
    }

}
