package edu.brown.hstore;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Queue;

import org.apache.log4j.Logger;

import edu.brown.hstore.Hstoreservice.Status;
import edu.brown.hstore.conf.HStoreConf;
import edu.brown.hstore.txns.AbstractTransaction;
import edu.brown.hstore.txns.LocalTransaction;
import edu.brown.hstore.txns.RemoteTransaction;
import edu.brown.interfaces.Shutdownable;
import edu.brown.logging.LoggerUtil;
import edu.brown.logging.LoggerUtil.LoggerBoolean;
import edu.brown.utils.ExceptionHandlingRunnable;
import edu.brown.utils.ThreadUtil;

/**
 * Simple thread that will rip through the deletable threads and remove txn handles
 * @author pavlo
 */
public class TransactionCleaner extends ExceptionHandlingRunnable implements Shutdownable {
    private static final Logger LOG = Logger.getLogger(TransactionCleaner.class);
    private static final LoggerBoolean debug = new LoggerBoolean();
    private static final LoggerBoolean trace = new LoggerBoolean();
    static {
        LoggerUtil.attachObserver(LOG, debug, trace);
    }
    
    private static final int LIMIT_PER_ROUND = 10000;
    private static final int NUM_REQUEUE_LISTS = 3;
    
    
    private final HStoreSite hstore_site;
    @SuppressWarnings("unused")
    private final HStoreConf hstore_conf;
    private boolean shutdown = false;
    private final Map<Long, AbstractTransaction> inflight_txns;
    
    /**
     * Queues for transactions that are ready to be cleaned up and deleted
     * There is one queue for each Status type
     */
    private final Queue<Long> deletables[];
    private final Status statuses[];
    
    /**
     * We'll maintain multiple sets of txns that need to get requeued for deletion.
     * We'll cycle through them to add in a natural delay for waiting until a txn
     * is fully ready to be deleted. This is probably only really necessary for distributed txns.
     */
    private final Collection<Long> requeues[][];
    
    /**
     * Constructor
     * @param hstore_site
     */
    @SuppressWarnings("unchecked")
    public TransactionCleaner(HStoreSite hstore_site) {
        this.hstore_site = hstore_site;
        this.hstore_conf = hstore_site.getHStoreConf();
        this.inflight_txns = hstore_site.getInflightTxns();
        this.statuses = new Status[Status.values().length];
        this.deletables = new Queue[this.statuses.length];
        this.requeues = new Collection[NUM_REQUEUE_LISTS][this.statuses.length];
        
        int i = 0;
        for (Entry<Status, Queue<Long>> e : hstore_site.getDeletableQueues().entrySet()) {
            this.statuses[i] = e.getKey();
            this.deletables[i] = e.getValue();
            for (int j = 0; j < this.requeues.length; j++) {
                this.requeues[j][i] = new ArrayList<Long>();
            } // FOR
            i += 1;
        } // FOR
    }

    @Override
    public void runImpl() {
        this.hstore_site.getThreadManager().registerProcessingThread();
        
        // Delete txn handles
        Long txn_id = null;
        int cur_index = 0;
        while (this.shutdown == false) {
            int swap_index = (cur_index + 1) % NUM_REQUEUE_LISTS;
            
            // if (hstore_conf.site.profiling) this.profiler.cleanup.start();
            boolean needsSleep = true;
            for (int i = 0; i < this.statuses.length; i++) {
                Status status = this.statuses[i];
                Queue<Long> queue = this.deletables[i];
                Collection<Long> swap_queue = this.requeues[swap_index][i];
                if (swap_queue.isEmpty() == false) {
                    queue.addAll(swap_queue);
                    swap_queue.clear();
                }
                
                Collection<Long> requeue = this.requeues[cur_index][i];
                int limit = LIMIT_PER_ROUND;
                while ((txn_id = queue.poll()) != null) {
                    // It's ok for us to not have a transaction handle, because it could be
                    // for a remote transaction that told us that they were going to need one
                    // of our partitions but then they never actually sent work to us
                    AbstractTransaction ts = this.inflight_txns.get(txn_id);
                    if (ts != null) {
                        assert(txn_id.equals(ts.getTransactionId())) :
                            String.format("Mismatched %s - Expected[%d] != Actual[%s]",
                                          ts, txn_id, ts.getTransactionId());
                        // We need to check whether a txn is ready to be deleted
                        if (ts.isDeletable()) {
                            if (ts instanceof RemoteTransaction) {
                                this.hstore_site.deleteRemoteTransaction((RemoteTransaction)ts, status);    
                            }
                            else {
                                this.hstore_site.deleteLocalTransaction((LocalTransaction)ts, status);
                            }
                            needsSleep = false;
                            limit--;
                        }
                        // We can't delete this yet, so we'll just stop checking
                        else {
                            if (trace.val)
                                LOG.trace(String.format("%s - Cannot delete %s at this point [status=%s]\n%s",
                                          ts, ts.getClass().getSimpleName(), status, ts.debug()));
                            requeue.add(txn_id);
                        }
                    } else if (debug.val) {
                        LOG.warn(String.format("Ignoring clean-up request for txn #%d because we do not have a handle " +
                                 "[status=%s]", txn_id, status));
                    }
                    if (limit <= 0) break;
                } // WHILE
            } // FOR
            if (needsSleep) ThreadUtil.sleep(10);
            cur_index = swap_index;
            // if (hstore_conf.site.profiling) this.profiler.cleanup.stop();
        } // WHILE
    }
    
    @Override
    public boolean isShuttingDown() {
        return (this.shutdown == true);
    }
    
    @Override
    public void shutdown() {
        this.shutdown = true;
    }

    @Override
    public void prepareShutdown(boolean error) {
        // Nothing to do...
    }

}
