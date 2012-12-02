package edu.brown.hstore.callbacks;

import org.apache.log4j.Logger;

import edu.brown.hstore.HStoreSite;
import edu.brown.hstore.Hstoreservice.Status;
import edu.brown.hstore.txns.AbstractTransaction;
import edu.brown.logging.LoggerUtil;
import edu.brown.logging.LoggerUtil.LoggerBoolean;
import edu.brown.utils.PartitionSet;

/**
 * Special callback that keeps track as to whether we have finished up
 * with everything that we need for a given transaction at a HStoreSite.
 * If we have, then we know it is safe to go ahead and call HStoreSite.deleteTransaction()
 * @author pavlo
 */
public class TransactionCleanupCallback extends AbstractTransactionCallback<AbstractTransaction, Integer, Integer> {
    private static final Logger LOG = Logger.getLogger(TransactionCleanupCallback.class);
    private static final LoggerBoolean debug = new LoggerBoolean(LOG.isDebugEnabled());
    static {
        LoggerUtil.attachObserver(LOG, debug);
    }
 
    private Status status;
    
    /**
     * Constructor
     * @param hstore_site
     */
    public TransactionCleanupCallback(HStoreSite hstore_site) {
        super(hstore_site);
    }

    public void init(AbstractTransaction ts, Status status, PartitionSet partitions) {
        // Only include local partitions
        int counter = 0;
        for (int p : hstore_site.getLocalPartitionIds().values()) {
            if (partitions.contains(p)) counter++;
        } // FOR
        assert(counter > 0);
        this.status = status;
        super.init(ts, counter, null);
    }
    
    
    @Override
    protected void unblockTransactionCallback() {
        hstore_site.queueDeleteTransaction(this.getTransactionId(), this.status);
    }
    
    @Override
    protected boolean abortTransactionCallback(Status status) {
        String msg = String.format("Unexpected %s abort for %s", this.getClass().getSimpleName(), this.ts);
        throw new RuntimeException(msg); 
    }
    
    @Override
    protected int runImpl(Integer partition) {
        return (1);
    }
}
