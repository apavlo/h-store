package edu.brown.hstore.dispatchers;

import org.apache.log4j.Logger;
import org.voltdb.StoredProcedureInvocation;
import org.voltdb.messaging.FastDeserializer;
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
        FastDeserializer fds = new FastDeserializer(p.getFirst());
        StoredProcedureInvocation invocation = null;
        try {
            invocation = fds.readObject(StoredProcedureInvocation.class);
        } catch (Exception ex) {
            LOG.fatal("Unexpected error when calling procedureInvocation!", ex);
            throw new RuntimeException(ex);
        }
        this.hStoreCoordinator.getHStoreSite().procedureInvocation(invocation, p.getFirst(), p.getSecond());
    }
}