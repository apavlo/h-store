package edu.brown.hstore.dtxn;

import java.util.Collection;
import java.util.Comparator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.PriorityBlockingQueue;

import org.apache.log4j.Logger;
import org.voltdb.TransactionIdManager;

import edu.brown.hstore.HStoreObjectPools;
import edu.brown.hstore.HStoreSite;
import edu.brown.hstore.Hstoreservice.Status;
import edu.brown.hstore.callbacks.TransactionInitWrapperCallback;
import edu.brown.hstore.interfaces.Loggable;
import edu.brown.hstore.interfaces.Shutdownable;
import edu.brown.hstore.util.TxnCounter;
import edu.brown.logging.LoggerUtil;
import edu.brown.logging.LoggerUtil.LoggerBoolean;
import edu.brown.statistics.Histogram;
import edu.brown.utils.CollectionUtil;

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
    
    private final Collection<Integer> localPartitions;
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
     * Maps txn IDs to their TransactionInitWrapperCallbacks
     */
    private final Map<Long, TransactionInitWrapperCallback> txn_callbacks = new ConcurrentHashMap<Long, TransactionInitWrapperCallback>();
    
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
        this.localPartitions = hstore_site.getLocalPartitionIds();
        assert(this.localPartitions.isEmpty() == false);
        this.localPartitionsArray = CollectionUtil.toIntArray(this.localPartitions);
        
        Collection<Integer> allPartitions = hstore_site.getAllPartitionIds();
        int num_ids = allPartitions.size();
        this.txn_queues = new TransactionInitPriorityQueue[num_ids];
        this.working_partitions = new boolean[this.txn_queues.length];
        this.last_txns = new Long[this.txn_queues.length];
        
        this.wait_time = hstore_site.getHStoreConf().site.txn_incoming_delay;
        for (int partition : allPartitions) {
            this.last_txns[partition] = -1l;
            if (this.localPartitions.contains(partition)) {
                this.txn_queues[partition] = new TransactionInitPriorityQueue(hstore_site, partition, this.wait_time);
                this.working_partitions[partition] = false;
                hstore_site.getStartWorkloadObservable().addObserver(this.txn_queues[partition]);
            }
        } // FOR
        
        if (d)
            LOG.debug(String.format("Created %d TransactionInitQueues for %s", num_ids, hstore_site.getSiteName()));
    }
    
    @Override
    public void updateLogging() {
        d = debug.get();
        t = trace.get();
    }
    
    public Histogram<Integer> getBlockedDtxnHistogram() {
        return this.blocked_hist;
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
        if (hstore_site.getHStoreConf().site.cpu_affinity) {
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
            TransactionInitWrapperCallback callback = null;
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
            
            if (this.last_txns[partition] != null && next_id.compareTo(this.last_txns[partition]) < 0) {
                if (t) LOG.trace(String.format("The next id for partition #%d is txn #%d but this is less than the previous txn #%d. Rejecting... [queueSize=%d]",
                                               partition, next_id, this.last_txns[partition], txn_queues[partition].size()));
                this.rejectTransaction(next_id, callback, Status.ABORT_RESTART, partition, last_txns[partition]);
                continue;
            }

            // otherwise send the init request to the specified partition
            if (t) LOG.trace(String.format("Good news! Partition #%d is ready to execute txn #%d! Invoking callback!",
                                           partition, next_id));
            this.last_txns[partition] = next_id;
            this.working_partitions[partition] = true;
            callback.run(partition);
            counter = callback.getCounter();
            
            txn_released = true;
                
            // remove the callback when this partition is the last one to start the job
            if (counter == 0) {
                if (d) LOG.debug(String.format("All local partitions needed by txn #%d are ready. Removing callback", next_id));
                this.cleanupTransaction(next_id);
            }
        } // FOR
        return txn_released;
    }
    

    /**
     * Remove the transaction from our internal queues
     * We will also put their TransactionInitWrapperCallback back into the
     * object pool (if they have one) 
     * @param txn_id
     */
    private void cleanupTransaction(Long txn_id) {
        TransactionInitWrapperCallback callback = this.txn_callbacks.remove(txn_id);
        
        if (callback != null) {
            for (Integer partition : callback.getPartitions()) {
                if (this.localPartitions.contains(partition) == false &&
                    this.last_txns[partition.intValue()].compareTo(txn_id) < 0) {
                    if (d) LOG.debug(String.format("Marking txn #%d as last txn id for remote partition %d",
                                                   txn_id, partition));
                    this.last_txns[partition] = txn_id;
                }
            } // FOR
            HStoreObjectPools.CALLBACKS_TXN_INITWRAPPER.returnObject(callback);
        }
    }
    
    private void rejectTransaction(Long txn_id, TransactionInitWrapperCallback callback, Status status, int reject_partition, Long reject_txnId) {
        // First send back an ABORT message to the initiating HStoreSite
        try {
            callback.abort(status, reject_partition, reject_txnId.longValue());
        } catch (Throwable ex) {
            String msg = "Unexpected error when trying to abort txn #" + txn_id;
            throw new RuntimeException(msg, ex);
        }
        
        // Then mark the txn as done at all the partitions that we set as
        // as the current txn. Not sure how this will work...
        for (Integer partition : callback.getPartitions()) {
            if (this.localPartitions.contains(partition) == false) continue;
            this.txn_queues[partition.intValue()].remove(txn_id);
            if (txn_id.equals(this.last_txns[partition.intValue()])) {
                this.finished(txn_id, status, partition.intValue());
            }
        } // FOR
        
        this.cleanupTransaction(txn_id);
    }
    
    /**
     * 
     * @param txn_id
     * @param partitions
     * @param callback
     */
    public boolean insert(Long txn_id, Collection<Integer> partitions, TransactionInitWrapperCallback callback) {
        return this.insert(txn_id, partitions, callback, false);
    }
    
    /**
     * Add a new transaction to this queue manager.
     * @param txn_id
     * @param partitions
     * @param callback
     * @param force
     * @return
     */
    public boolean insert(Long txn_id, Collection<Integer> partitions, TransactionInitWrapperCallback callback, boolean force) {
        if (d) LOG.debug(String.format("Adding new distributed txn #%d into queue [force=%s, partitions=%s]",
                                       txn_id, force, partitions));
        
        this.txn_callbacks.put(txn_id, callback);
        boolean should_notify = false;
        boolean ret = true;
        for (Integer partition : partitions) {
            TransactionInitPriorityQueue queue = this.txn_queues[partition.intValue()];
            
            // We can pre-emptively check whether this txnId is greater than
            // the largest one that we know about at a remote partition
            if (this.localPartitions.contains(partition) == false) {
                if (this.last_txns[partition.intValue()].compareTo(txn_id) > 0) {
                    if (t) LOG.trace(String.format("The last txn for remote partition is #%d but this is greater than our txn #%d. Rejecting...",
                                                   partition, this.last_txns[partition], txn_id));
                    this.rejectTransaction(txn_id, callback, Status.ABORT_RESTART, partition, this.last_txns[partition]);
                    ret = false;
                    break;
                }
                continue;
            }
            
            Long next_safe = queue.noteTransactionRecievedAndReturnLastSeen(txn_id.longValue());
            if (next_safe.compareTo(txn_id) > 0) {
                if (t) LOG.trace(String.format("The next safe id for partition #%d is txn #%d but this is less than our new txn #%d. Rejecting...",
                                            partition, next_safe, txn_id));
                this.rejectTransaction(txn_id, callback, Status.ABORT_RESTART, partition, next_safe);
                ret = false;
                break;
            } else if (queue.offer(txn_id, false) == false) {
                if (t) LOG.trace(String.format("The DTXN queue partition #%d is overloaded. Throttling txn #%d",
                                            partition, next_safe, txn_id));
                this.rejectTransaction(txn_id, callback, Status.ABORT_THROTTLED, partition, next_safe);
                ret = false;
                break;
            } else if (!this.working_partitions[partition]) {
                should_notify = true;
            }
            
            if (t) LOG.trace(String.format("Added txn #%d to queue for partition %d [working=%s, queueSize=%d]",
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
        
        // Always remove it from this partition's queue
        if (this.txn_queues[partition].remove(txn_id) == false) {
            // Then free up the partition if it was actually running
            if (this.last_txns[partition] != null && this.last_txns[partition].equals(txn_id)) {
                this.working_partitions[partition] = false;
            }
            synchronized (this) {
                notifyAll();
            } // SYNCH
        }
    }
    
    protected boolean isEmpty() {
        for (int i = 0; i < this.txn_queues.length; ++i) {
            if (this.txn_queues[i].isEmpty() == false) return (false);
        }
        return (true);
    }

    public Long getLastTransaction(int partition) {
        return (this.last_txns[partition]);
    }
    public Long getCurrentTransaction(int partition) {
        if (working_partitions[partition]) {
            return (this.last_txns[partition]);
        }
        return (null);
    }
    
    /**
     * 
     * @param partition
     * @param txn_id
     */
    public synchronized void markAsLastTxnId(int partition, Long txn_id) {
        if (this.last_txns[partition] == null || this.last_txns[partition].compareTo(txn_id) < 0) {
            if (d) LOG.debug(String.format("Marking txn #%d as last txn id for remote partition %d", txn_id, partition));
            this.last_txns[partition] = txn_id;
        }
    }
    
    /**
     * A LocalTransaction from this HStoreSite is blocked because a remote HStoreSite that it needs to
     * access a partition on has its last tranasction id as greater than what the LocalTransaction was issued.
     * @param ts
     * @param partition
     * @param txn_id
     */
    public void queueBlockedDTXN(LocalTransaction ts, int partition, Long txn_id) {
        if (d) LOG.debug(String.format("Blocking %s until after a txn greater than #%d is created for partition %d",
                                                 ts, txn_id, partition));
        if (this.blocked_dtxn_release.putIfAbsent(ts, txn_id) != null) {
            Long other_txn_id = this.blocked_dtxn_release.get(ts);
            if (other_txn_id != null && other_txn_id.compareTo(txn_id) < 0) {
                this.blocked_dtxn_release.put(ts, txn_id);
            }
        } else {
            this.blocked_dtxns.offer(ts);
        }
        if (this.localPartitions.contains(partition) == false) {
            this.markAsLastTxnId(partition, txn_id);
        }
        if (hstore_site.getHStoreConf().site.status_show_txn_info && ts.getRestartCounter() == 1) {
            TxnCounter.BLOCKED_REMOTE.inc(ts.getProcedure());
            this.blocked_hist.put((int)TransactionIdManager.getInitiatorIdFromTransactionId(txn_id));
        }
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
                if (d) LOG.debug(String.format("Releasing blocked %s because the lastest txn was #%d [release=%d]",
                                               ts, last_txn_id, releaseTxnId));
                this.blocked_dtxns.remove();
                this.blocked_dtxn_release.remove(ts);
                hstore_site.transactionRestart(ts, Status.ABORT_RESTART, true);
            } else break;
        } // WHILE
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
    public TransactionInitWrapperCallback getCallback(long txn_id) {
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
}
