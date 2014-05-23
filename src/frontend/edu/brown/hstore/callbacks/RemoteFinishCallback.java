package edu.brown.hstore.callbacks;

import org.apache.log4j.Logger;

import edu.brown.hstore.HStoreSite;
import edu.brown.hstore.Hstoreservice.Status;
import edu.brown.hstore.txns.AbstractTransaction;
import edu.brown.logging.LoggerUtil;
import edu.brown.logging.LoggerUtil.LoggerBoolean;
import edu.brown.utils.PartitionSet;
import org.voltdb.exceptions.EvictedTupleAccessException;
/**
 * Special callback that keeps track as to whether we have finished up
 * with everything that we need for a given transaction at a HStoreSite.
 * If we have, then we know it is safe to go ahead and call HStoreSite.deleteTransaction()
 * @author pavlo
 */
public class RemoteFinishCallback extends PartitionCountingCallback<AbstractTransaction> {
    private static final Logger LOG = Logger.getLogger(RemoteFinishCallback.class);
    private static final LoggerBoolean debug = new LoggerBoolean();
    static {
        LoggerUtil.attachObserver(LOG, debug);
    }
    
    private final PartitionSet localPartitions = new PartitionSet();
 
    // ----------------------------------------------------------------------------
    // INTIALIZATION
    // ----------------------------------------------------------------------------
    
    /**
     * Constructor
     * @param hstore_site
     */
    public RemoteFinishCallback(HStoreSite hstore_site) {
        super(hstore_site);
    }

    public void init(AbstractTransaction ts, PartitionSet partitions) {
        // Remove non-local partitions
        this.localPartitions.clear();
        this.localPartitions.addAll(partitions);
        this.localPartitions.retainAll(this.hstore_site.getLocalPartitionIds());
        super.init(ts, this.localPartitions);
    }
    
    // ----------------------------------------------------------------------------
    // CALLBACK METHODS
    // ----------------------------------------------------------------------------

    @Override
    protected void unblockCallback() {
   //     this.hstore_site.queueDeleteTransaction(this.ts.getTransactionId(), this.ts.getStatus());
}
    
    @Override
    protected void abortCallback(int partition, Status status) {
        String msg = String.format("Unexpected %s abort for %s", this.getClass().getSimpleName(), this.ts);
        throw new RuntimeException(msg); 
    }
}
