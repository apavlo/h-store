package edu.mit.hstore.callbacks;

import java.util.Collection;

import org.apache.log4j.Logger;

import edu.brown.hstore.Hstore;
import edu.brown.logging.LoggerUtil;
import edu.brown.logging.LoggerUtil.LoggerBoolean;
import edu.mit.hstore.HStoreSite;
import edu.mit.hstore.dtxn.RemoteTransaction;

public class TransactionCleanupCallback extends BlockingCallback<Integer, Integer> {
    private static final Logger LOG = Logger.getLogger(TransactionCleanupCallback.class);
    private final static LoggerBoolean debug = new LoggerBoolean(LOG.isDebugEnabled());
    private final static LoggerBoolean trace = new LoggerBoolean(LOG.isTraceEnabled());
    static {
        LoggerUtil.attachObserver(LOG, debug, trace);
    }
 
    private RemoteTransaction ts;
    private Hstore.Status status;
    
    /**
     * Constructor
     * @param hstore_site
     */
    public TransactionCleanupCallback(HStoreSite hstore_site) {
        super(hstore_site, false);
    }

    public void init(RemoteTransaction ts, Hstore.Status status, Collection<Integer> partitions) {
        if (debug.get())
            LOG.debug("Initializing " + this.getClass().getSimpleName() + " for " + ts);
        
        // Only include local partitions
        int counter = 0;
        Collection<Integer> localPartitions = hstore_site.getLocalPartitionIds();
        for (Integer p : partitions) {
            if (localPartitions.contains(p)) counter++;
        } // FOR
        assert(counter > 0);
        
        this.ts = ts;
        super.init(ts.getTransactionId(), counter, null);
    }
    
    @Override
    protected void finishImpl() {
        this.ts = null;
    }
    
    @Override
    public boolean isInitialized() {
        return (this.ts != null && super.isInitialized());
    }
    
    @Override
    protected void unblockCallback() {
        hstore_site.completeTransaction(txn_id, status);
    }
    
    @Override
    protected void abortCallback(Hstore.Status status) {
        assert(false) :
            String.format("Unexpected %s abort for %s", this.getClass().getSimpleName(), this.ts); 
    }
    
    @Override
    protected int runImpl(Integer partition) {
        return (1);
    }
}
