package edu.brown.hstore.dtxn;

import java.util.Collection;
import java.util.Comparator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.PriorityBlockingQueue;

import org.apache.commons.collections15.map.ListOrderedMap;
import org.apache.log4j.Logger;
import org.voltdb.TransactionIdManager;

import edu.brown.hstore.HStoreObjectPools;
import edu.brown.hstore.HStoreSite;
import edu.brown.hstore.Hstoreservice.Status;
import edu.brown.hstore.callbacks.TransactionInitQueueCallback;
import edu.brown.hstore.conf.HStoreConf;
import edu.brown.hstore.interfaces.Loggable;
import edu.brown.hstore.interfaces.Shutdownable;
import edu.brown.hstore.util.TxnCounter;
import edu.brown.logging.LoggerUtil;
import edu.brown.logging.LoggerUtil.LoggerBoolean;
import edu.brown.statistics.Histogram;
import edu.brown.utils.CollectionUtil;
import edu.brown.utils.StringUtil;

public class TransactionQueueManager implements Runnable, Loggable, Shutdownable {
    private static final Logger LOG = Logger.getLogger(TransactionQueueManager.class);
    private final static LoggerBoolean debug = new LoggerBoolean(LOG.isDebugEnabled());
    private final static LoggerBoolean trace = new LoggerBoolean(LOG.isTraceEnabled());
    static {
        LoggerUtil.attachObserver(LOG, debug, trace);
    }
    private static boolean d = debug.get();
    private static boolean t = trace.get();
    
    /**
     * the site that will send init requests to this coordinator
     */
    private final HStoreSite hstore_site;
    private final HStoreConf hstore_conf;
    
    private final int localPartitionsArray[];
    
    private boolean stop = false;
    
    /**
     * 
     */
    private final long wait_time;
    
    /**
     * Contains one queue for every partition managed by this coordinator
     */
    private final TransactionInitPriorityQueue[] txn_queues;
    
    /**
     * The last txn ID that was executed for each partition
     * Our local partitions must be accurate, but we can be off for the remote ones
     */
    private final Long[] last_txns;
    
    /**
     * Indicates which partitions are currently executing a distributed transaction
     */
    private final boolean[] working_partitions;
    
    /**
     * Maps txn IDs to their TransactionInitQueueCallbacks
     */
    private final Map<Long, TransactionInitQueueCallback> txn_callbacks = new ConcurrentHashMap<Long, TransactionInitQueueCallback>();
    
    /**
     * Blocked Queue Comparator
     */
    private Comparator<LocalTransaction> blocked_comparator = new Comparator<LocalTransaction>() {
        @Override
        public int compare(LocalTransaction o0, LocalTransaction o1) {
            Long txnId0 = blocked_dtxn_release.get(o0);
            Long txnId1 = blocked_dtxn_release.get(o1);
            if (txnId0 == null && txnId1 == null) return (0);
            if (txnId0 == null) return (1);
            if (txnId1 == null) return (-1);
            if (txnId0.equals(txnId1) == false) return (txnId0.compareTo(txnId1));
            return (int)(o0.getClientHandle() - o1.getClientHandle());
        }
    };
    
    private ConcurrentHashMap<LocalTransaction, Long> blocked_dtxn_release = new ConcurrentHashMap<LocalTransaction, Long>();
    
    /**
     * Internal list of distributed LocalTransactions that are unable to
     * get the locks that they need on the remote partitions
     */
    private PriorityBlockingQueue<LocalTransaction> blocked_dtxns = new PriorityBlockingQueue<LocalTransaction>(100, blocked_comparator);
    
    /**
     * This Histogram keeps track of what sites have blocked the most transactions from us
     */
    private Histogram<Integer> blocked_hist = new Histogram<Integer>();
    
    /**
     * Constructor
     * @param hstore_site
     */
    public TransactionQueueManager(HStoreSite hstore_site) {
        this.hstore_site = hstore_site;
        this.hstore_conf = hstore_site.getHStoreConf();
        
        Collection<Integer> allPartitions = hstore_site.getAllPartitionIds();
        int num_ids = allPartitions.size();
        this.txn_queues = new TransactionInitPriorityQueue[num_ids];
        this.working_partitions = new boolean[this.txn_queues.length];
        this.last_txns = new Long[this.txn_queues.length];
        this.localPartitionsArray = CollectionUtil.toIntArray(hstore_site.getLocalPartitionIds());
        this.wait_time = hstore_conf.site.txn_incoming_delay;
        
        // Allocate transaction queues
        for (int partition : allPartitions) {
            this.last_txns[partition] = -1l;
            if (this.hstore_site.isLocalPartition(partition)) {
                this.txn_queues[partition] = new TransactionInitPriorityQueue(hstore_site, partition, this.wait_time);
                this.working_partitions[partition] = false;
                hstore_site.getStartWorkloadObservable().addObserver(this.txn_queues[partition]);
            }
        } // FOR
        
        if (d) LOG.debug(String.format("Created %d TransactionInitQueues for %s",
                                       num_ids, hstore_site.getSiteName()));
    }
    
    @Override
    public void updateLogging() {
        d = debug.get();
        t = trace.get();
    }
    
    /**
     * Every time this thread gets waken up, it locks the queues, loops through the txn_queues, and looks at the lowest id in each queue.
     * If any id is lower than the last_txn id for that partition, it gets rejected and sent back to the caller.
     * Otherwise, the lowest txn_id is popped off and sent to the corresponding partition.
     * Then the thread unlocks the queues and goes back to sleep.
     * If all the partitions are now busy, the thread will wake up when one of them is finished.
     * Otherwise, it will wake up when something else gets added to a queue.
     */
    @Override
    public void run() {
        Thread self = Thread.currentThread();
        self.setName(HStoreSite.getThreadName(hstore_site, "queue"));
        if (hstore_conf.site.cpu_affinity) {
            hstore_site.getThreadManager().registerProcessingThread();
        }
        
        if (d) LOG.debug("Starting distributed transaction queue manager thread");
        
        final TransactionIdManager idManager = hstore_site.getTransactionIdManager();
        long txn_id = -1;
        long last_id = -1;
        
        while (this.stop == false) {
            synchronized (this) {
                try {
                    this.wait(this.wait_time * 10); // FIXME
                } catch (InterruptedException e) {
                    // Nothing...
                }
            } // SYNCH
            if (t) LOG.trace("Checking partition queues for dtxns to release!");
            while (this.checkQueues()) {
                // Keep checking the queue as long as they have more stuff in there
                // for us to process
            }
            if (this.blocked_dtxns.isEmpty() == false) {
                txn_id = idManager.getLastTxnId();
                if (last_id == txn_id) txn_id = idManager.getNextUniqueTransactionId();
                this.checkBlockedDTXNs(txn_id);
                last_id = txn_id;
            }
        }
    }
    
    /**
     * Check whether there are any transactions that need to be released for execution
     * at the partitions controlled by this queue manager
     * Returns true if we released a transaction at at least one partition
     */
    protected boolean checkQueues() {
        if (t) LOG.trace("Checking queues");
        
        boolean txn_released = false;
        for (int partition : this.localPartitionsArray) {
            TransactionInitQueueCallback callback = null;
            Long next_id = null;
            int counter = -1;
            
            if (this.working_partitions[partition]) {
                if (t) LOG.trace(String.format("Partition #%d is already executing a transaction. Skipping...", partition));
                continue;
            }
            TransactionInitPriorityQueue queue = this.txn_queues[partition];
            next_id = queue.poll();
            // If null, then there is nothing that is ready to run at this partition,
            // so we'll just skip to the next one
            if (next_id == null) {
                if (t) LOG.trace(String.format("Partition #%d does not have a transaction ready to run. Skipping... [queueSize=%d]",
                                               partition, txn_queues[partition].size()));
                continue;
            }
            
            callback = this.txn_callbacks.get(next_id);
            assert(callback != null) : "Unexpected null callback for txn #" + next_id;
            
            // If this callback has already been aborted, then there is nothing we need to
            // do. Somebody else will make sure that this txn is removed from the queue
            // We will always want to return true to keep trying to get the next transaction
            if (callback.isAborted()) {
                if (d) LOG.debug(String.format("The next id for partition #%d is txn #%d but its callback is marked as aborted. [queueSize=%d]",
                                               partition, next_id, this.last_txns[partition], txn_queues[partition].size()));
                queue.remove(next_id);
                txn_released = true;
                continue;
            }
            
            // We have to acquire a lock to make sure that somebody else isn't screwing with us
            // This is some funkiness due to how we are cleaning up transactions...
            synchronized (this.last_txns[partition]) {
                if (this.last_txns[partition].compareTo(next_id) > 0) {
                    if (d) LOG.debug(String.format("The next id for partition #%d is txn #%d but this is less than the previous txn #%d. Rejecting... [queueSize=%d]",
                                                   partition, next_id, this.last_txns[partition], txn_queues[partition].size()));
                    this.rejectTransaction(next_id, callback, Status.ABORT_RESTART, partition, this.last_txns[partition]);
                    continue;
                }

                if (d) LOG.debug(String.format("Good news! Partition #%d is ready to execute txn #%d! Invoking callback!",
                                               partition, next_id));
                this.last_txns[partition] = next_id;
                this.working_partitions[partition] = true;
            } // SYNCH
            // Send the init request for the specified partition
            try {
                callback.run(partition);
                counter = callback.getCounter();
            } catch (Throwable ex) {
                throw new RuntimeException(String.format("Failed to invoke %s for txn #%d at partition %d",
                                                         callback.getClass().getSimpleName(), next_id, partition), ex);
            }
            txn_released = true;
                
            // remove the callback when this partition is the last one to start the job
            if (counter == 0) {
                if (d) LOG.debug(String.format("All local partitions needed by txn #%d are ready. Removing callback", next_id));
                this.cleanupTransaction(next_id);
            }
        } // FOR
        return txn_released;
    }
    
    
    // ----------------------------------------------------------------------------
    // PUBLIC API
    // ----------------------------------------------------------------------------
    
    /**
     * Add a new transaction to this queue manager.
     * @param txn_id
     * @param partitions
     * @param callback
     * @return
     */
    public boolean insert(Long txn_id, Collection<Integer> partitions, TransactionInitQueueCallback callback) {
        if (d) LOG.debug(String.format("Adding new distributed txn #%d into queue [partitions=%s]",
                                       txn_id, partitions));
        
        this.txn_callbacks.put(txn_id, callback);
        boolean should_notify = false;
        boolean ret = true;
        for (int partition : partitions) {
            TransactionInitPriorityQueue queue = this.txn_queues[partition];
            
            // We can pre-emptively check whether this txnId is greater than
            // the largest one that we know about at a remote partition
            if (this.hstore_site.isLocalPartition(partition) == false) {
                // We don't need to acquire the lock on last_txns at this partition because 
                // all that we care about is that whatever value is in there now is greater than
                // the what the transaction was trying to use.
                if (this.last_txns[partition].compareTo(txn_id) > 0) {
                    if (d) LOG.debug(String.format("The last txn for remote partition is #%d but this is greater than our txn #%d. Rejecting...",
                                                   partition, this.last_txns[partition], txn_id));
                    this.rejectTransaction(txn_id, callback, Status.ABORT_RESTART, partition, this.last_txns[partition]);
                    ret = false;
                    break;
                }
                continue;
            }
            
            Long next_safe = queue.noteTransactionRecievedAndReturnLastSeen(txn_id.longValue());
            if (next_safe.compareTo(txn_id) > 0) {
                if (d) LOG.debug(String.format("The next safe id for partition #%d is txn #%d but this is greater than our new txn #%d. Rejecting...",
                                               partition, next_safe, txn_id));
                this.rejectTransaction(txn_id, callback, Status.ABORT_RESTART, partition, next_safe);
                ret = false;
                break;
            } else if (queue.offer(txn_id, false) == false) {
                if (d) LOG.debug(String.format("The DTXN queue partition #%d is overloaded. Throttling txn #%d",
                                               partition, next_safe, txn_id));
                this.rejectTransaction(txn_id, callback, Status.ABORT_THROTTLED, partition, next_safe);
                ret = false;
                break;
            } else if (!this.working_partitions[partition]) {
                should_notify = true;
            }
            
            if (d) LOG.debug(String.format("Added txn #%d to queue for partition %d [working=%s, queueSize=%d]",
                                           txn_id, partition, this.working_partitions[partition], this.txn_queues[partition].size()));
        } // FOR
        if (should_notify) {
            synchronized (this) {
                this.notifyAll();
            } // SYNCH
        }
        return (ret);
    }
    
    /**
     * Mark the transaction as being finished with the given partition
     * @param txn_id
     * @param partition
     */
    public void finished(Long txn_id, Status status, int partition) {
        if (d) LOG.debug(String.format("Marking txn #%d as finished on partition %d [status=%s, basePartition=%d]",
                                    txn_id, partition, status,
                                    TransactionIdManager.getInitiatorIdFromTransactionId(txn_id)));
        
        // XXX: Do we need to always check whether it is actually running?
        boolean poke = false;
        synchronized (this.last_txns[partition]) {
            if (this.last_txns[partition].equals(txn_id)) {
                if (d) LOG.debug(String.format("Unmarked partition %d as working because txn #%d is finished",
                                 partition, txn_id));
                this.working_partitions[partition] = false;
                poke = true;
            } else if (d) {
                LOG.debug(String.format("Not unmarking partition %d for txn #%d [current=%d, locked=%s]",
                                        partition, txn_id, this.last_txns[partition], this.working_partitions[partition]));
            }
        } // SYNCH
        
        // Always remove it from this partition's queue
        // If this remove() returns false, then we know that our transaction wasn't
        // sitting in the queue for that partition.
        if (this.txn_queues[partition].remove(txn_id)) {
            // This is a local transaction that was still waiting for this partition. That means
            // we will want to decrement the counter its Transaction
            if (d) LOG.debug(String.format("Removed txn #%d from parititon %d queue", txn_id, partition));
            TransactionInitQueueCallback callback = this.txn_callbacks.get(txn_id);
            assert(callback != null) :
                "Missing TransactionInitQueueCallback for txn #" + txn_id;
            callback.abort(status);
            if (callback.decrementCounter(1) == 0) {
                this.cleanupTransaction(txn_id);
            }
        }
        if (poke) {
            synchronized (this) {
                this.notifyAll();
            } // SYNCH
        }
    }
    
    // ----------------------------------------------------------------------------
    // BLOCKED DTXN QUEUE MANAGEMENT
    // ----------------------------------------------------------------------------
    
    /**
     * Mark the last transaction seen at a remote partition in the cluster
     * @param partition
     * @param txn_id
     */
    public void markAsLastTxnId(int partition, Long txn_id) {
        assert(this.hstore_site.isLocalPartition(partition) == false) :
            "Trying to mark the last seen txnId for local partition #" + partition;
        synchronized (this.last_txns[partition]) {
            if (this.last_txns[partition].compareTo(txn_id) < 0) {
                if (d) LOG.debug(String.format("Marking txn #%d as last txnId for remote partition %d", txn_id, partition));
                this.last_txns[partition] = txn_id;
            }
        } // SYNCH
    }
    
    /**
     * A LocalTransaction from this HStoreSite is blocked because a remote HStoreSite that it needs to
     * access a partition on has its last tranasction id as greater than what the LocalTransaction was issued.
     * @param ts
     * @param partition
     * @param last_txn_id
     */
    public void queueBlockedDTXN(LocalTransaction ts, int partition, Long last_txn_id) {
        if (d) LOG.debug(String.format("%s - Blocking transaction until after a txnId greater than #%d is created for partition %d",
                                       ts, last_txn_id, partition));
       
        // IMPORTANT: Mark this transaction as needing to be restarted
        //            This will prevent it from getting deleted out from under us
        ts.setNeedsRestart(true);
        
        if (this.blocked_dtxn_release.putIfAbsent(ts, last_txn_id) != null) {
            Long other_txn_id = this.blocked_dtxn_release.get(ts);
            if (other_txn_id != null && other_txn_id.compareTo(last_txn_id) < 0) {
                this.blocked_dtxn_release.put(ts, last_txn_id);
            }
        } else {
            this.blocked_dtxns.offer(ts);
        }
        if (hstore_site.isLocalPartition(partition) == false) {
            this.markAsLastTxnId(partition, last_txn_id);
        }
        if (hstore_conf.site.status_show_txn_info && ts.getRestartCounter() == 1) {
            TxnCounter.BLOCKED_REMOTE.inc(ts.getProcedure());
            this.blocked_hist.put((int)TransactionIdManager.getInitiatorIdFromTransactionId(last_txn_id));
        }
        synchronized (this) {
            this.notifyAll();
        } // SYNCH
    }
    
    /**
     * 
     * @param last_txn_id
     */
    private void checkBlockedDTXNs(Long last_txn_id) {
        if (d && this.blocked_dtxns.isEmpty() == false)
            LOG.debug(String.format("Checking whether we can release %d blocked dtxns [lastTxnId=%d]",
                                    this.blocked_dtxns.size(), last_txn_id));
        
        while (this.blocked_dtxns.isEmpty() == false) {
            LocalTransaction ts = this.blocked_dtxns.peek();
            Long releaseTxnId = this.blocked_dtxn_release.get(ts);
            if (releaseTxnId == null) {
                if (d) LOG.warn("Missing release TxnId for " + ts);
                this.blocked_dtxns.remove();
                continue;
            }
            if (releaseTxnId.compareTo(last_txn_id) < 0) {
                if (d) LOG.debug(String.format("Releasing blocked %s because the lastest txnId was #%d [release=%d]",
                                               ts, last_txn_id, releaseTxnId));
                this.blocked_dtxns.remove();
                this.blocked_dtxn_release.remove(ts);
                hstore_site.transactionRestart(ts, Status.ABORT_RESTART);
                ts.setNeedsRestart(false);
                if (ts.isDeletable()) {
                    hstore_site.deleteTransaction(ts.getTransactionId(), Status.ABORT_REJECT);
                }
                
            } else break;
        } // WHILE
    }
    
    // ----------------------------------------------------------------------------
    // INTERNAL METHODS
    // ----------------------------------------------------------------------------
    
    /**
     * Remove the transaction from our internal queues
     * We will also put their TransactionInitQueueCallback back into the
     * object pool (if they have one) 
     * @param txn_id
     */
    private void cleanupTransaction(Long txn_id) {
        if (d) LOG.debug(String.format("Cleaning up txn #%d from queue manager", txn_id));
        TransactionInitQueueCallback callback = this.txn_callbacks.remove(txn_id);
        
        if (callback != null) {
            for (int partition : callback.getPartitions()) {
                // Skip any local partition
                if (hstore_site.isLocalPartition(partition)) continue;
                synchronized (this.last_txns[partition]) {
                    if (this.last_txns[partition].compareTo(txn_id) < 0) {
                        if (d) LOG.debug(String.format("Marking txn #%d as last txnId for remote partition %d",
                                                       txn_id, partition));
                        this.last_txns[partition] = txn_id;
                    }
                } // SYNCH
            } // FOR
            HStoreObjectPools.CALLBACKS_TXN_INITQUEUE.returnObject(callback);
        }
    }
    
    private void rejectTransaction(Long txn_id, TransactionInitQueueCallback callback, Status status, int reject_partition, Long reject_txnId) {
        if (d) LOG.debug(String.format("Announcing that txn #%d is rejected on partition %d until a txnId greater than #%d",
                                       txn_id, reject_partition, reject_txnId));
        
        // First send back an ABORT message to the initiating HStoreSite
        try {
            callback.abort(status, reject_partition, reject_txnId);
        } catch (Throwable ex) {
            String msg = String.format("Unexpected error when trying to abort txn #%d [status=%s, rejectPartition=%d, rejectTxnId=%s]",
                                      txn_id, status, reject_partition, reject_txnId);
            throw new RuntimeException(msg, ex);
        }
        
        // Then mark the txn as done at all the partitions that we set as
        // as the current txn. Not sure how this will work...
        for (int partition : callback.getPartitions()) {
            if (hstore_site.isLocalPartition(partition) == false) continue;
            this.txn_queues[partition].remove(txn_id);
            synchronized (this.last_txns[partition]) {
                if (txn_id.equals(this.last_txns[partition])) {
                    this.finished(txn_id, status, partition);
                }
            } // SYNCH
        } // FOR
        
        this.cleanupTransaction(txn_id);
    }
    
    // ----------------------------------------------------------------------------
    // UTILITY METHODS
    // ----------------------------------------------------------------------------
    
    protected boolean isEmpty() {
        for (int i = 0; i < this.txn_queues.length; ++i) {
            if (this.txn_queues[i].isEmpty() == false) return (false);
        }
        return (true);
    }

    public Long getLastTransaction(int partition) {
        return (this.last_txns[partition]);
    }
    
    /**
     * Return the current transaction that is executing at this partition
     * @param partition
     * @return
     */
    public Long getCurrentTransaction(int partition) {
        if (this.working_partitions[partition]) {
            return (this.last_txns[partition]);
        }
        return (null);
    }
    
    public Histogram<Integer> getBlockedDtxnHistogram() {
        return this.blocked_hist;
    }
    
    /**
     * DEBUG METHOD
     */
    public int getQueueSize(int partition) {
        return (this.txn_queues[partition].size());
    }
    /**
     * DEBUG METHOD
     */
    public TransactionInitPriorityQueue getQueue(int partition) {
        return (this.txn_queues[partition]);
    }
    /**
     * DEBUG METHOD
     */
    public TransactionInitQueueCallback getCallback(long txn_id) {
        return (this.txn_callbacks.get(txn_id));
    }

    @Override
    public void prepareShutdown(boolean error) {
        // Nothing for now
        // Probably should abort all queued txns.
    }

    @Override
    public void shutdown() {
        this.stop = true;
    }

    @Override
    public boolean isShuttingDown() {
        return (this.stop);
    }
    
    @Override
    public String toString() {
        @SuppressWarnings("unchecked")
        Map<String, Object> m[] = (Map<String, Object>[])new Map[2];
        int idx = -1;

        // Basic Information
        m[++idx] = new ListOrderedMap<String, Object>();
        m[idx].put("Wait Time", this.wait_time + " ms");
        m[idx].put("# of Callbacks", this.txn_callbacks.size());
        m[idx].put("# of Blocked Txns", this.blocked_dtxns.size());
        
        // Local Partitions
        m[++idx] = new ListOrderedMap<String, Object>();
        for (int p = 0; p < this.last_txns.length; p++) {
            Map<String, Object> inner = new ListOrderedMap<String, Object>();
            inner.put("Current Txn", this.last_txns[p]);
            if (hstore_site.isLocalPartition(p)) {
                inner.put("Locked?", this.working_partitions[p]);
                inner.put("Queue Size", this.txn_queues[p].size());
            }
            m[idx].put(String.format("Partition #%02d", p), inner);
        } // FOR
        
        return StringUtil.formatMaps(m);
    }
}
