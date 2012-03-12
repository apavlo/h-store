package edu.brown.hstore.dtxn;

import java.util.Collection;
import java.util.Comparator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

import org.apache.commons.collections15.map.ListOrderedMap;
import org.apache.log4j.Logger;
import org.voltdb.TransactionIdManager;

import edu.brown.hstore.HStoreObjectPools;
import edu.brown.hstore.HStoreSite;
import edu.brown.hstore.HStoreThreadManager;
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
    
    private final Semaphore checkFlag = new Semaphore(1);
    
    /**
     * 
     */
    private final long wait_time;
    
    // ----------------------------------------------------------------------------
    // TRANSACTION INIT QUEUES
    // ----------------------------------------------------------------------------
    
    /**
     * Contains one queue for every partition managed by this coordinator
     */
    private final TransactionInitPriorityQueue[] initQueues;
    
    /**
     * The last txn ID that was executed for each partition
     * Our local partitions must be accurate, but we can be off for the remote ones
     */
    private final Long[] initQueuesLastTxn;
    
    /**
     * Indicates which partitions are currently executing a distributed transaction
     */
    private final boolean[] initQueuesBlocked;
    
    /**
     * Maps txn IDs to their TransactionInitQueueCallbacks
     */
    private final Map<Long, TransactionInitQueueCallback> initQueuesCallbacks = new ConcurrentHashMap<Long, TransactionInitQueueCallback>();
    
    // ----------------------------------------------------------------------------
    // BLOCKED DISTRIBUTED TRANSACTIONS
    // ----------------------------------------------------------------------------
    
    /**
     * Blocked Queue Comparator
     */
    private Comparator<LocalTransaction> blockedComparator = new Comparator<LocalTransaction>() {
        @Override
        public int compare(LocalTransaction o0, LocalTransaction o1) {
            Long txnId0 = blockedQueueTransactions.get(o0);
            Long txnId1 = blockedQueueTransactions.get(o1);
            if (txnId0 == null && txnId1 == null) return (0);
            if (txnId0 == null) return (1);
            if (txnId1 == null) return (-1);
            if (txnId0.equals(txnId1) == false) return (txnId0.compareTo(txnId1));
            return (int)(o0.getClientHandle() - o1.getClientHandle());
        }
    };

    /**
     * Internal list of distributed LocalTransactions that are unable to
     * get the locks that they need on the remote partitions
     */
    private final PriorityBlockingQueue<LocalTransaction> blockedQueue = new PriorityBlockingQueue<LocalTransaction>(100, blockedComparator);

    /**
     * TODO: Merge with blockedQueue
     */
    private final ConcurrentHashMap<LocalTransaction, Long> blockedQueueTransactions = new ConcurrentHashMap<LocalTransaction, Long>();
    
    /**
     * This Histogram keeps track of what sites have blocked the most transactions from us
     */
    private final Histogram<Integer> blockedQueueHistogram = new Histogram<Integer>();
    
    // ----------------------------------------------------------------------------
    // TRANSACTIONS THAT NEED TO BE REQUEUED
    // ----------------------------------------------------------------------------

    /**
     * A queue of aborted transactions that need to restart and add back into the system
     * <B>NOTE:</B> Anything that shows up in this queue will be deleted by this manager
     */
    private final LinkedBlockingQueue<LocalTransaction> restartQueue = new LinkedBlockingQueue<LocalTransaction>(); 
    
    // ----------------------------------------------------------------------------
    // INTIALIZATION
    // ----------------------------------------------------------------------------
    
    /**
     * Constructor
     * @param hstore_site
     */
    public TransactionQueueManager(HStoreSite hstore_site) {
        this.hstore_site = hstore_site;
        this.hstore_conf = hstore_site.getHStoreConf();
        
        Collection<Integer> allPartitions = hstore_site.getAllPartitionIds();
        int num_ids = allPartitions.size();
        this.initQueues = new TransactionInitPriorityQueue[num_ids];
        this.initQueuesBlocked = new boolean[this.initQueues.length];
        this.initQueuesLastTxn = new Long[this.initQueues.length];
        this.localPartitionsArray = CollectionUtil.toIntArray(hstore_site.getLocalPartitionIds());
        this.wait_time = hstore_conf.site.txn_incoming_delay;
        
        // Allocate transaction queues
        for (int partition : allPartitions) {
            this.initQueuesLastTxn[partition] = -1l;
            if (this.hstore_site.isLocalPartition(partition)) {
                this.initQueues[partition] = new TransactionInitPriorityQueue(hstore_site, partition, this.wait_time);
                this.initQueuesBlocked[partition] = false;
                hstore_site.getStartWorkloadObservable().addObserver(this.initQueues[partition]);
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
        self.setName(HStoreThreadManager.getThreadName(hstore_site, "queue"));
        if (hstore_conf.site.cpu_affinity) {
            hstore_site.getThreadManager().registerProcessingThread();
        }
        
        if (d) LOG.debug("Starting distributed transaction queue manager thread");
        
        final TransactionIdManager idManager = hstore_site.getTransactionIdManager();
        long txn_id = -1;
        long last_id = -1;
        
        while (this.stop == false) {
            try {
                this.checkFlag.tryAcquire(this.wait_time*2, TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
                // Nothing...
            }
            
            if (t) LOG.trace("Checking partition queues for dtxns to release!");
            while (this.checkQueues()) {
                // Keep checking the queue as long as they have more stuff in there
                // for us to process
            }
            if (this.blockedQueue.isEmpty() == false) {
                txn_id = idManager.getLastTxnId();
                if (last_id == txn_id) txn_id = idManager.getNextUniqueTransactionId();
                this.checkBlockedQueue(txn_id);
                last_id = txn_id;
            }
            
            // Requeue mispredicted local transactions
            this.checkRestartQueue();
        } // WHILE
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
            
            if (this.initQueuesBlocked[partition]) {
                if (t) LOG.trace(String.format("Partition #%d is already executing a transaction. Skipping...", partition));
                continue;
            }

            // Poll the queue and get the next value. We need
            // a lock in case somebody is looking for this txnId to remove
            synchronized (this.initQueues[partition]) {
                next_id = this.initQueues[partition].poll();    
            } // SYNCH
            
            // If null, then there is nothing that is ready to run at this partition,
            // so we'll just skip to the next one
            if (next_id == null) {
                if (t) LOG.trace(String.format("Partition #%d initQueue does not have a transaction ready to run. Skipping... [queueSize=%d]",
                                               partition, this.initQueues[partition].size()));
                continue;
            }
            
            callback = this.initQueuesCallbacks.get(next_id);
            assert(callback != null) : "Unexpected null callback for txn #" + next_id;
            
            // If this callback has already been aborted, then there is nothing we need to
            // do. Somebody else will make sure that this txn is removed from the queue
            // We will always want to return true to keep trying to get the next transaction
            if (callback.isAborted()) {
                if (d) LOG.debug(String.format("The next id for partition #%d is txn #%d but its callback is marked as aborted. [queueSize=%d]",
                                               partition, next_id, this.initQueuesLastTxn[partition], initQueues[partition].size()));
                this.initQueues[partition].remove(next_id);
                txn_released = true;
                continue;
            }
            // We don't need to acquire lock here because we know that our partition isn't doing
            // anything at this moment. 
            else if (this.initQueuesLastTxn[partition].compareTo(next_id) > 0) {
                if (d) LOG.debug(String.format("The next id for partition #%d is txn #%d but this is less than the previous txn #%d. Rejecting... [queueSize=%d]",
                                               partition, next_id, this.initQueuesLastTxn[partition], initQueues[partition].size()));
                this.rejectTransaction(next_id, callback, Status.ABORT_RESTART, partition, this.initQueuesLastTxn[partition]);
                continue;
            }

            if (d) LOG.debug(String.format("Good news! Partition #%d is ready to execute txn #%d! Invoking initQueue callback!",
                                           partition, next_id));
            this.initQueuesLastTxn[partition] = next_id;
            this.initQueuesBlocked[partition] = true;
            
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
        return (txn_released);
    }
    
    
    // ----------------------------------------------------------------------------
    // PUBLIC API
    // ----------------------------------------------------------------------------
    
    /**
     * Add a new transaction to this queue manager.
     * Returns true if the transaction was successfully inserted at all partitions
     * @param txn_id
     * @param partitions
     * @param callback
     * @return
     */
    public boolean initInsert(Long txn_id, Collection<Integer> partitions, TransactionInitQueueCallback callback) {
        if (d) LOG.debug(String.format("Adding new distributed txn #%d into initQueue [partitions=%s]",
                                       txn_id, partitions));
        
        // Always put in the callback first, because we may end up rejecting
        // this txnId in the loop below
        this.initQueuesCallbacks.put(txn_id, callback);
        
        boolean should_notify = false;
        boolean ret = true;
        for (int partition : partitions) {
            // We can pre-emptively check whether this txnId is greater than
            // the largest one that we know about at a partition
            // We don't need to acquire the lock on last_txns at this partition because 
            // all that we care about is that whatever value is in there now is greater than
            // the what the transaction was trying to use.
            if (this.initQueuesLastTxn[partition].compareTo(txn_id) > 0) {
                if (d) LOG.debug(String.format("The last initQueue txnId for remote partition is #%d but this is greater than our txn #%d. Rejecting...",
                                               partition, this.initQueuesLastTxn[partition], txn_id));
                this.rejectTransaction(txn_id, callback, Status.ABORT_RESTART, partition, this.initQueuesLastTxn[partition]);
                ret = false;
                break;
            }
            
            // There's nothing that we need to do for a remote partition
            if (this.hstore_site.isLocalPartition(partition) == false) continue;
            
            // For local partitions, peek ahead in this partition's queue to see whether the 
            // txnId that we're trying to insert is less than the next one that we expect to release 
            TransactionInitPriorityQueue queue = this.initQueues[partition];
            Long next_safe = queue.noteTransactionRecievedAndReturnLastSeen(txn_id);
            
            // The next txnId that we're going to try to execute is already greater
            // than this new txnId that we were given! Rejection!
            if (next_safe.compareTo(txn_id) > 0) {
                if (d) LOG.debug(String.format("The next safe initQueue txnId for partition #%d is txn #%d but this is greater than our new txn #%d. Rejecting...",
                                               partition, next_safe, txn_id));
                this.rejectTransaction(txn_id, callback, Status.ABORT_RESTART, partition, next_safe);
                ret = false;
                break;
            }
            // Our queue is overloaded. We have to throttle the txnId!
            else if (queue.offer(txn_id, false) == false) {
                if (d) LOG.debug(String.format("The initQueue for partition #%d is overloaded. Throttling txn #%d",
                                               partition, next_safe, txn_id));
                this.rejectTransaction(txn_id, callback, Status.ABORT_THROTTLED, partition, next_safe);
                ret = false;
                break;
            }
            // If our queue is currently idle, poke the thread so that it wakes up and tries to
            // schedule our boys!
            else if (this.initQueuesBlocked[partition] == false) {
                should_notify = true;
            }
            
            if (d) LOG.debug(String.format("Added txn #%d to initQueue for partition %d [locked=%s, queueSize=%d]",
                                           txn_id, partition, this.initQueuesBlocked[partition], this.initQueues[partition].size()));
        } // FOR
        if (should_notify && this.checkFlag.availablePermits() == 0)
            this.checkFlag.release();
        return (ret);
    }
    
    /**
     * Mark the transaction as being finished with the given local partition. This can be called
     * either before or after the transaction was initialized at all partitions.
     * @param txn_id
     * @param partition
     */
    public void initFinished(Long txn_id, Status status, int partition) {
        assert(this.hstore_site.isLocalPartition(partition)) :
            "Trying to mark txn #" + txn_id + " as finished on remote partition #" + partition;
        if (d) LOG.debug(String.format("Notifying queue manager that txn #%d is finished on partition %d " +
        		                       "[status=%s, basePartition=%d]",
        		                       txn_id, partition, status,
        		                       TransactionIdManager.getInitiatorIdFromTransactionId(txn_id)));
        
        // If the given txnId is the current transaction at this partition and still holds
        // the lock on the partition, then we will want to release it
        boolean poke = false;
        if (this.initQueuesBlocked[partition] && this.initQueuesLastTxn[partition].equals(txn_id)) {
            if (d) LOG.debug(String.format("Unlocking partition %d because txn #%d is finished [status=%s]",
                                           partition, txn_id, status));
            this.initQueuesBlocked[partition] = false;
            poke = true;
        } else if (d) {
            LOG.debug(String.format("Not unlocking partition %d for txn #%d [current=%d, locked=%s, status=%s]",
                                    partition, txn_id, this.initQueuesLastTxn[partition], this.initQueuesBlocked[partition], status));
        }
        
        // Always attempt to remove it from this partition's queue
        // If this remove() returns false, then we know that our transaction wasn't
        // sitting in the queue for that partition.
        boolean removed = false;
        synchronized (this.initQueues[partition]) {
            removed = this.initQueues[partition].remove(txn_id);
        } // SYNCH
        // This is a local transaction that is still waiting for this partition (i.e., it hasn't
        // been rejected yet). That means we will want to decrement the counter its Transaction
        if (removed) {
            if (d) LOG.debug(String.format("Removed txn #%d from partition %d queue", txn_id, partition));
            TransactionInitQueueCallback callback = this.initQueuesCallbacks.get(txn_id);
            assert(callback != null) :
                "Missing TransactionInitQueueCallback for txn #" + txn_id;
            if (callback.isAborted() == false) callback.abort(status);
        }
        if (poke && this.checkFlag.availablePermits() == 0)
            this.checkFlag.release();
    }
    

    /**
     * Get the last tranasction id that was initialized at the given partition
     */
    public Long getLastInitTransaction(int partition) {
        return (this.initQueuesLastTxn[partition]);
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
        // At this point we can safely assume that the callback is either aborted
        // or its internal counter is zero. Basically there is nothing else that we
        // need to do with this transaction at this point.
        TransactionInitQueueCallback callback = this.initQueuesCallbacks.remove(txn_id);
        if (callback != null) {
            if (d) LOG.debug(String.format("Returned %s for txn #%d back to object pool",
                                           callback.getClass().getSimpleName(), txn_id));
            HStoreObjectPools.CALLBACKS_TXN_INITQUEUE.returnObject(callback);
        }
    }
    
    /**
     * Reject the given transaction at this QueueManager.
     * @param txn_id
     * @param callback
     * @param status
     * @param reject_partition
     * @param reject_txnId
     */
    private void rejectTransaction(Long txn_id, TransactionInitQueueCallback callback, Status status, int reject_partition, Long reject_txnId) {
        if (d) LOG.debug(String.format("Rejecting txn #%d on partition %d. Blocking until a txnId greater than #%d",
                                       txn_id, reject_partition, reject_txnId));
        
        // First send back an ABORT message to the initiating HStoreSite (if we haven't already)
        if (callback.isAborted() == false) {
            try {
                callback.abort(status, reject_partition, reject_txnId);
            } catch (Throwable ex) {
                String msg = String.format("Unexpected error when trying to abort txn #%d [status=%s, rejectPartition=%d, rejectTxnId=%s]",
                                          txn_id, status, reject_partition, reject_txnId);
                throw new RuntimeException(msg, ex);
            }
        }
        
        // Then make sure that our txnId is removed from all of the local partitions
        // that we queued it on.
        boolean poke = false;
        for (int partition : callback.getPartitions()) {
            if (hstore_site.isLocalPartition(partition) == false) continue;
            // Try to remove it from our queue. If we can't then it might
            // be that we're the current transaction at this partition, so that
            // we need to make sure that we release the locks
            boolean removed = false;
            synchronized (this.initQueues[partition]) {
                removed = this.initQueues[partition].remove(txn_id);
            } // SYNCH
            
            // We don't need to acquire a lock here because we know that
            // nobody else can update us unless the lock flag is false
            if (removed == false && this.initQueuesBlocked[partition] && this.initQueuesLastTxn[partition].equals(txn_id)) {
                this.initQueuesBlocked[partition] = false;
                poke = true;
            }
        } // FOR
        if (poke && this.checkFlag.availablePermits() == 0)
            this.checkFlag.release();
        this.cleanupTransaction(txn_id);
    }
    
    // ----------------------------------------------------------------------------
    // BLOCKED DTXN QUEUE MANAGEMENT
    // ----------------------------------------------------------------------------
    
    /**
     * Mark the last transaction seen at a remote partition in the cluster
     * @param partition
     * @param txn_id
     */
    public void markLastTransaction(int partition, Long txn_id) {
        assert(this.hstore_site.isLocalPartition(partition) == false) :
            "Trying to mark the last seen txnId for local partition #" + partition;
        
        // This lock is low-contention because we don't update the last txnId seen
        // at partitions very often.
        synchronized (this.initQueuesLastTxn[partition]) {
            if (this.initQueuesLastTxn[partition].compareTo(txn_id) < 0) {
                if (d) LOG.debug(String.format("Marking txn #%d as last txnId for remote partition %d", txn_id, partition));
                this.initQueuesLastTxn[partition] = txn_id;
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
    public void blockTransaction(LocalTransaction ts, int partition, Long last_txn_id) {
        if (d) LOG.debug(String.format("%s - Blocking transaction until after a txnId greater than #%d is created for partition %d",
                                       ts, last_txn_id, partition));
       
        // IMPORTANT: Mark this transaction as needing to be restarted
        //            This will prevent it from getting deleted out from under us
        ts.setNeedsRestart(true);
        
        if (this.blockedQueueTransactions.putIfAbsent(ts, last_txn_id) != null) {
            Long other_txn_id = this.blockedQueueTransactions.get(ts);
            if (other_txn_id != null && other_txn_id.compareTo(last_txn_id) < 0) {
                this.blockedQueueTransactions.put(ts, last_txn_id);
            }
        } else {
            this.blockedQueue.offer(ts);
        }
        if (hstore_site.isLocalPartition(partition) == false) {
            this.markLastTransaction(partition, last_txn_id);
        }
        if (hstore_conf.site.status_show_txn_info && ts.getRestartCounter() == 1) {
            TxnCounter.BLOCKED_REMOTE.inc(ts.getProcedure());
            this.blockedQueueHistogram.put((int)TransactionIdManager.getInitiatorIdFromTransactionId(last_txn_id));
        }
        if (this.checkFlag.availablePermits() == 0)
            this.checkFlag.release();
    }
    
    /**
     * 
     * @param last_txn_id
     */
    private void checkBlockedQueue(Long last_txn_id) {
        if (d && this.blockedQueue.isEmpty() == false)
            LOG.debug(String.format("Checking whether we can release %d blocked dtxns [lastTxnId=%d]",
                                    this.blockedQueue.size(), last_txn_id));
        
        while (this.blockedQueue.isEmpty() == false) {
            LocalTransaction ts = this.blockedQueue.peek();
            Long releaseTxnId = this.blockedQueueTransactions.get(ts);
            if (releaseTxnId == null) {
                if (d) LOG.warn("Missing release TxnId for " + ts);
                this.blockedQueue.remove(ts);
                continue;
            }
            if (releaseTxnId.compareTo(last_txn_id) < 0) {
                if (d) LOG.debug(String.format("Releasing blocked %s because the lastest txnId was #%d [release=%d]",
                                               ts, last_txn_id, releaseTxnId));
                this.blockedQueue.remove();
                this.blockedQueueTransactions.remove(ts);
                hstore_site.transactionRestart(ts, Status.ABORT_RESTART);
                ts.setNeedsRestart(false);
                if (ts.isDeletable()) {
                    hstore_site.deleteTransaction(ts.getTransactionId(), Status.ABORT_REJECT);
                }
                
            } else break;
        } // WHILE
    }
    
    // ----------------------------------------------------------------------------
    // RESTART QUEUE MANAGEMENT
    // ----------------------------------------------------------------------------

    /**
     * Queue a transaction that was aborted so it can be restarted later on.
     * This is a non-blocking call.
     * @param ts
     * @param status
     */
    public void restartTransaction(LocalTransaction ts, Status status) {
        if (d) LOG.debug(String.format("%s - Requeing transaction for execution [status=%s]", ts, status));
        
        // HACK: Store the status in the embedded ClientResponse
        ts.getClientResponse().setStatus(status);
        if (this.restartQueue.offer(ts) == false) {
            this.hstore_site.transactionReject(ts, Status.ABORT_REJECT);
            ts.markAsDeletable();
            this.hstore_site.deleteTransaction(ts.getTransactionId(), Status.ABORT_REJECT);
        }
        if (this.checkFlag.availablePermits() == 0)
            this.checkFlag.release();
    }
    
    private void checkRestartQueue() {
        LocalTransaction ts = null;
        while ((ts = this.restartQueue.poll()) != null) {
            Status status = ts.getClientResponse().getStatus();
            this.hstore_site.transactionRestart(ts, status);
            ts.markAsDeletable();
            this.hstore_site.deleteTransaction(ts.getTransactionId(), status);
        } // WHILE

    }
    
    // ----------------------------------------------------------------------------
    // UTILITY METHODS
    // ----------------------------------------------------------------------------
    
    protected boolean isEmpty() {
        for (int i = 0; i < this.initQueues.length; ++i) {
            if (this.initQueues[i].isEmpty() == false) return (false);
        }
        return (true);
    }
    
    /**
     * Return the current transaction that is executing at this partition
     * @param partition
     * @return
     */
    public Long getCurrentTransaction(int partition) {
        if (this.initQueuesBlocked[partition]) {
            return (this.initQueuesLastTxn[partition]);
        }
        return (null);
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
    
    // ----------------------------------------------------------------------------
    // DEBUG METHODS
    // ----------------------------------------------------------------------------

    public class DebugContext {
        public int getInitQueueSize() {
            int ctr = 0;
            for (int p : localPartitionsArray)
              ctr += initQueues[p].size();
            return (ctr);
        }
        public int getInitQueueSize(int partition) {
            return (initQueues[partition].size());
        }
        public TransactionInitPriorityQueue getInitQueue(int partition) {
            return (initQueues[partition]);
        }
        public TransactionInitQueueCallback getInitCallback(Long txn_id) {
            return (initQueuesCallbacks.get(txn_id));
        }
        public int getBlockedQueueSize() {
            return (blockedQueue.size());
        }
        public int getRestartQueueSize() {
            return (restartQueue.size());
        }
        public Histogram<Integer> getBlockedDtxnHistogram() {
            return (blockedQueueHistogram);
        }
    }
    
    public TransactionQueueManager.DebugContext getDebugContext() {
        return new TransactionQueueManager.DebugContext();
    }
    
    @Override
    public String toString() {
        @SuppressWarnings("unchecked")
        Map<String, Object> m[] = (Map<String, Object>[])new Map[2];
        int idx = -1;

        // Basic Information
        m[++idx] = new ListOrderedMap<String, Object>();
        m[idx].put("Wait Time", this.wait_time + " ms");
        m[idx].put("# of Callbacks", this.initQueuesCallbacks.size());
        m[idx].put("# of Blocked Txns", this.blockedQueue.size());
        
        // Local Partitions
        m[++idx] = new ListOrderedMap<String, Object>();
        for (int p = 0; p < this.initQueuesLastTxn.length; p++) {
            Map<String, Object> inner = new ListOrderedMap<String, Object>();
            inner.put("Current Txn", this.initQueuesLastTxn[p]);
            if (hstore_site.isLocalPartition(p)) {
                inner.put("Locked?", this.initQueuesBlocked[p]);
                inner.put("Queue Size", this.initQueues[p].size());
            }
            m[idx].put(String.format("Partition #%02d", p), inner);
        } // FOR
        
        return StringUtil.formatMaps(m);
    }
    
}
