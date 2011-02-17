package org.voltdb;

import java.util.Collection;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.log4j.Logger;

import edu.brown.utils.CollectionUtil;
import edu.brown.utils.LoggerUtil;
import edu.mit.hstore.HStoreSite;
import edu.mit.hstore.dtxn.TransactionState;

/**
 * 
 * @author pavlo
 */
public class ExecutionSiteHelper implements Runnable {
    public static final Logger LOG = Logger.getLogger(ExecutionSiteHelper.class);
    private final static AtomicBoolean debug = new AtomicBoolean(LOG.isDebugEnabled());
    private final static AtomicBoolean trace = new AtomicBoolean(LOG.isTraceEnabled());
    static {
        LoggerUtil.attachObserver(LOG, debug, trace);
    }
    
    /**
     * How many milliseconds will we keep around old transaction states
     */
    private final int txn_expire;
    /**
     * The maximum number of transactions to clean up per poll round
     */
    private final int txn_per_round;
    /**
     * The sites we need to invoke cleanupTransaction() + tick() for
     */
    private final Collection<ExecutionSite> sites;
    /**
     * Set to false after the first time we are invoked
     */
    private boolean first = true;
    
    /**
     * 
     * @param sites
     */
    public ExecutionSiteHelper(Collection<ExecutionSite> sites, int max_txn_per_round, int txn_expire) {
        this.sites = sites;
        this.txn_expire = txn_expire;
        this.txn_per_round = max_txn_per_round;
    }
    
    @Override
    public synchronized void run() {
        final boolean d = debug.get();
        final boolean t = trace.get();
        
        if (this.first) {
            Thread self = Thread.currentThread();
            HStoreSite hstore_site = CollectionUtil.getFirst(this.sites).hstore_site;
            self.setName(hstore_site.getThreadName("help"));
            this.first = false;
        }
        if (d) LOG.debug("New invocation of the ExecutionSiteHelper. Let's clean-up some txns!");
        
        long to_remove = System.currentTimeMillis() - this.txn_expire;
        for (ExecutionSite es : this.sites) {
            if (d) LOG.debug(String.format("Partition %d has %d finished transactions", es.partitionId, es.finished_txn_states.size()));
            
            int cleaned = 0;
            while (es.finished_txn_states.isEmpty() == false &&
                    (this.txn_per_round < 0 || cleaned < this.txn_per_round)) {
                TransactionState ts = es.finished_txn_states.peek();
                if (ts.getFinishedTimestamp() < to_remove) {
                    if (t) LOG.trace("Cleaning txn #" + ts.getTransactionId());
                    es.cleanupTransaction(ts);
                    es.finished_txn_states.remove();
                    cleaned++;
                } else break;
            } // WHILE
            if (d) LOG.debug(String.format("Cleaned %d TransactionStates at partition %d", cleaned, es.partitionId));
            // Only call tick here!
            es.tick();
        } // FOR
    }

}
