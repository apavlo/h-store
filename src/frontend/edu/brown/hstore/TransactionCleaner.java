package edu.brown.hstore;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentLinkedQueue;
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
import edu.brown.utils.ThreadUtil;

/**
 * Simple thread that will rip through the deletable threads and remove txn handles
 * @author pavlo
 */
public class TransactionCleaner implements Runnable, Shutdownable {
    private static final Logger LOG = Logger.getLogger(TransactionCleaner.class);
    private static final LoggerBoolean debug = new LoggerBoolean(LOG.isDebugEnabled());
    private static final LoggerBoolean trace = new LoggerBoolean(LOG.isTraceEnabled());
    static {
        LoggerUtil.attachObserver(LOG, debug, trace);
    }
    
    private final HStoreSite hstore_site;
    @SuppressWarnings("unused")
    private final HStoreConf hstore_conf;
    private boolean shutdown = false;
    private final Map<Long, AbstractTransaction> inflight_txns;
    
    /**
     * Queues for transactions that are ready to be cleaned up and deleted
     * There is one queue for each Status type
     */
    private final Map<Status, Queue<Long>> deletable_txns;
    
    private final Map<Status, Queue<Long>> deletable_txns_requeue[];
    private int deletable_txns_index = 0;
    
    /**
     * Constructor
     * @param hstore_site
     */
    @SuppressWarnings("unchecked")
    public TransactionCleaner(HStoreSite hstore_site) {
        this.hstore_site = hstore_site;
        this.hstore_conf = hstore_site.getHStoreConf();
        this.deletable_txns = hstore_site.getDeletableQueues();
        this.inflight_txns = hstore_site.getInflightTxns();
        
        this.deletable_txns_requeue = new Map[2];
        for (int i = 0; i < this.deletable_txns_requeue.length; i++) {
            this.deletable_txns_requeue[i] = new HashMap<Status, Queue<Long>>();
            for (Status s : Status.values()) {
                this.deletable_txns_requeue[i].put(s, new ConcurrentLinkedQueue<Long>());
            } // FOR
        } // FOR
    }

    @Override
    public void run() {
        this.hstore_site.getThreadManager().registerProcessingThread();
        
        // Delete txn handles
        Long txn_id = null;
        while (this.shutdown == false) {
            int cur_index = this.deletable_txns_index;
            int swap_index = (cur_index == 1 ? 0 : 1);
            
            // if (hstore_conf.site.profiling) this.profiler.cleanup.start();
            boolean needsSleep = true;
            for (Entry<Status, Queue<Long>> e : this.deletable_txns.entrySet()) {
                Status status = e.getKey();
                Queue<Long> queue = e.getValue();
                if (this.deletable_txns_requeue[swap_index].isEmpty() == false) {
                    Queue<Long> swap_queue = this.deletable_txns_requeue[swap_index].get(status);
                    queue.addAll(swap_queue);
                    swap_queue.clear();
                }
                
                Queue<Long> requeue = this.deletable_txns_requeue[cur_index].get(status);
                int limit = 10000;
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
            this.deletable_txns_index = (this.deletable_txns_index == 1 ? 0 : 1);
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
