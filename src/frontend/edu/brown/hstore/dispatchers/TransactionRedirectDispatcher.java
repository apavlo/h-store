package edu.brown.hstore.dispatchers;

import org.apache.log4j.Logger;
import org.voltdb.utils.Pair;

import edu.brown.hstore.HStoreCoordinator;
import edu.brown.hstore.callbacks.TransactionRedirectResponseCallback;

/**
 * 
 */
public class TransactionRedirectDispatcher extends AbstractDispatcher<Pair<byte[], TransactionRedirectResponseCallback>> {
    private static final Logger LOG = Logger.getLogger(TransactionRedirectDispatcher.class);
    
    public TransactionRedirectDispatcher(HStoreCoordinator hStoreCoordinator) {
        super(hStoreCoordinator);
    }

    @Override
    public void runImpl(Pair<byte[], TransactionRedirectResponseCallback> p) {
        this.hstore_coordinator.getHStoreSite().procedureInvocation(p.getFirst(), p.getSecond());
    }
}