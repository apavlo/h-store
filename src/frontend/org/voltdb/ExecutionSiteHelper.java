package org.voltdb;

import java.util.Collection;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.log4j.Logger;

import edu.brown.utils.LoggerUtil;
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
    private static final int FINISHED_TRANSACTION_GARBAGE_COLLECTION = 2000;
    
    /**
     * The maximum number of transactions to clean up per poll round
     */
    private static final int MAX_TRANSACTION_GARBAGE_COLLECTION = 1000;
    

    /**
     * The sites we need to invoke cleanupTransaction() + tick() for
     */
    private final Collection<ExecutionSite> sites;
    
    /**
     * 
     * @param sites
     */
    public ExecutionSiteHelper(Collection<ExecutionSite> sites) {
        this.sites = sites;
    }
    
    @Override
    public synchronized void run() {
        long to_remove = System.currentTimeMillis() - FINISHED_TRANSACTION_GARBAGE_COLLECTION;
        for (ExecutionSite es : this.sites) {
          int cleaned = 0;
          while (es.finished_txn_states.isEmpty() == false &&
                 cleaned < MAX_TRANSACTION_GARBAGE_COLLECTION) {
              TransactionState ts = es.finished_txn_states.peek();
              if (ts.getFinishedTimestamp() < to_remove) {
                  es.cleanupTransaction(ts);
                  es.finished_txn_states.remove();
                  cleaned++;
              } else break;
          } // WHILE
          if (cleaned > 0 && debug.get()) LOG.debug("Cleaned " + cleaned + " transaction states");
          // Only call tick here!
          es.tick();
        } // FOR
    }

}
