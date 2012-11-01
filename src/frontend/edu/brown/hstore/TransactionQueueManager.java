package edu.brown.hstore;

import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;
import org.voltdb.CatalogContext;
import org.voltdb.TransactionIdManager;
import org.voltdb.exceptions.ServerFaultException;
import org.voltdb.utils.EstTimeUpdater;
import org.voltdb.utils.Pair;

import com.google.protobuf.RpcCallback;

import edu.brown.hstore.Hstoreservice.Status;
import edu.brown.hstore.Hstoreservice.TransactionInitResponse;
import edu.brown.hstore.callbacks.TransactionInitCallback;
import edu.brown.hstore.callbacks.TransactionInitQueueCallback;
import edu.brown.hstore.conf.HStoreConf;
import edu.brown.hstore.txns.AbstractTransaction;
import edu.brown.hstore.txns.LocalTransaction;
import edu.brown.hstore.util.TransactionCounter;
import edu.brown.interfaces.Configurable;
import edu.brown.interfaces.DebugContext;
import edu.brown.interfaces.Loggable;
import edu.brown.interfaces.Shutdownable;
import edu.brown.logging.LoggerUtil;
import edu.brown.logging.LoggerUtil.LoggerBoolean;
import edu.brown.profilers.TransactionQueueManagerProfiler;
import edu.brown.statistics.Histogram;
import edu.brown.utils.EventObservable;
import edu.brown.utils.EventObserver;
import edu.brown.utils.PartitionSet;
import edu.brown.utils.StringUtil;

public class TransactionQueueManager implements Runnable, Loggable, Shutdownable, Configurable {
    private static final Logger LOG = Logger.getLogger(TransactionQueueManager.class);
    private static final LoggerBoolean debug = new LoggerBoolean(LOG.isDebugEnabled());
    private static final LoggerBoolean trace = new LoggerBoolean(LOG.isTraceEnabled());
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
    
    private final PartitionSet localPartitions;
    
    private boolean stop = false;
    
    private final Semaphore checkFlag = new Semaphore(1);
    
    /**
     * 
     */
    private long wait_time;
    
    private final TransactionQueueManagerProfiler profiler;
    
    // ----------------------------------------------------------------------------
    // TRANSACTION PARTITION LOCKS QUEUES
    // ----------------------------------------------------------------------------
    
    /**
     * Contains one queue for every partition managed by this coordinator
     */
    private final TransactionInitPriorityQueue[] lockQueues;
    
    /**
     * The last txns that was executed for each partition
     * Our local partitions must be accurate, but we can be off for the remote ones.
     * This should be just the transaction id, since the AbstractTransaction handles
     * could have been cleaned up by the time we need this data.
     */
    private final Long[] lockQueuesLastTxn;
    
    /**
     * Indicates which partitions are currently executing a distributed transaction
     */
    private final boolean[] lockQueuesBlocked;
    
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
    private final PriorityBlockingQueue<LocalTransaction> blockedQueue = 
            new PriorityBlockingQueue<LocalTransaction>(100, blockedComparator);

    /**
     * TODO: Merge with blockedQueue
     */
    private final ConcurrentHashMap<LocalTransaction, Long> blockedQueueTransactions = 
            new ConcurrentHashMap<LocalTransaction, Long>();
    
    /**
     * This Histogram keeps track of what sites have blocked the most transactions from us
     */
    private final Histogram<Integer> blockedQueueHistogram = new Histogram<Integer>();
    
    // ----------------------------------------------------------------------------
    // TRANSACTIONS THAT NEED TO INIT
    // ----------------------------------------------------------------------------

    /**
     * A queue of transactions that need to be "init-ed" using the HStoreCoordinator
     */
    private final Queue<LocalTransaction> initQueue = new ConcurrentLinkedQueue<LocalTransaction>(); 
    
    // ----------------------------------------------------------------------------
    // TRANSACTIONS THAT NEED TO BE REQUEUED
    // ----------------------------------------------------------------------------

    /**
     * A queue of aborted transactions that need to restart and add back into the system
     * <B>NOTE:</B> Anything that shows up in this queue will be deleted by this manager
     */
    private final Queue<Pair<LocalTransaction, Status>> restartQueue =
            new ConcurrentLinkedQueue<Pair<LocalTransaction, Status>>(); 
    
    // ----------------------------------------------------------------------------
    // INTIALIZATION
    // ----------------------------------------------------------------------------
    
    /**
     * Constructor
     * @param hstore_site
     */
    public TransactionQueueManager(HStoreSite hstore_site) {
        CatalogContext catalogContext = hstore_site.getCatalogContext();
        PartitionSet allPartitions = catalogContext.getAllPartitionIds();
        int num_partitions = allPartitions.size();
        
        this.hstore_site = hstore_site;
        this.hstore_conf = hstore_site.getHStoreConf();
        this.profiler = new TransactionQueueManagerProfiler(num_partitions);
        this.lockQueues = new TransactionInitPriorityQueue[num_partitions];
        this.lockQueuesBlocked = new boolean[this.lockQueues.length];
        this.lockQueuesLastTxn = new Long[this.lockQueues.length];
        this.localPartitions = hstore_site.getLocalPartitionIds();
        this.updateConf(this.hstore_conf);
        
        // Allocate transaction queues
        for (int partition : allPartitions.values()) {
            this.lockQueuesLastTxn[partition] = Long.valueOf(-1);
            if (this.hstore_site.isLocalPartition(partition)) {
                this.lockQueues[partition] = new TransactionInitPriorityQueue(hstore_site, partition, this.wait_time);
                this.lockQueuesBlocked[partition] = false;
            }
        } // FOR
        
        // Add a EventObservable that will tell us when the first non-sysproc
        // request arrives from a client. This will then tell the queues that its ok
        // to increase their limits if they're empty
        EventObservable<HStoreSite> observable = hstore_site.getStartWorkloadObservable();
        observable.addObserver(new EventObserver<HStoreSite>() {
            public void update(EventObservable<HStoreSite> o, HStoreSite arg) {
                for (TransactionInitPriorityQueue queue : lockQueues) {
                    if (queue != null) queue.setAllowIncrease(true);
                } // FOR
            };
        });
        
        if (d) LOG.debug(String.format("Created %d TransactionInitQueues for %s",
                         num_partitions, hstore_site.getSiteName()));
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
        self.setName(HStoreThreadManager.getThreadName(hstore_site, HStoreConstants.THREAD_NAME_TXNQUEUE));
        this.hstore_site.getThreadManager().registerProcessingThread();
        
        if (d) LOG.debug("Starting distributed transaction queue manager thread");
        
        while (this.stop == false) {
            if (hstore_conf.site.queue_profiling) profiler.idle.start();
            try {
                // this.checkFlag.tryAcquire(this.wait_time*2, TimeUnit.MILLISECONDS);
                this.checkFlag.tryAcquire(5, TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
                // Nothing...
            } finally {
                if (hstore_conf.site.queue_profiling && profiler.idle.isStarted()) profiler.idle.stop();
            }
            
            if (t) LOG.trace("Checking partition queues for dtxns to release!");
            if (hstore_conf.site.queue_profiling) profiler.lock_queue.start();
            while (this.checkLockQueues()) {
                // Keep checking the queue as long as they have more stuff in there
                // for us to process
            }
            if (hstore_conf.site.queue_profiling && profiler.lock_queue.isStarted()) profiler.lock_queue.stop();
            
            // Release transactions for initialization to the HStoreCoordinator
            if (hstore_conf.site.queue_profiling) profiler.init_queue.start();
            this.checkInitQueue();
            if (hstore_conf.site.queue_profiling && profiler.init_queue.isStarted()) profiler.init_queue.stop();
            
            // Release blocked distributed transactions
            if (hstore_conf.site.queue_profiling) profiler.block_queue.start();
            this.checkBlockedQueue();
            if (hstore_conf.site.queue_profiling && profiler.block_queue.isStarted()) profiler.block_queue.stop();
            
            // Requeue mispredicted local transactions
            if (hstore_conf.site.queue_profiling) profiler.restart_queue.start();
            this.checkRestartQueue();
            if (hstore_conf.site.queue_profiling && profiler.restart_queue.isStarted()) profiler.restart_queue.stop();
            
            // Count the number of unique concurrent dtxns
            if (hstore_conf.site.queue_profiling) {
                profiler.concurrent_dtxn_ids.clear();
                for (int partition: this.localPartitions.values()) {
                    if (this.lockQueuesLastTxn[partition].longValue() >= 0) {
                        profiler.concurrent_dtxn_ids.add(this.lockQueuesLastTxn[partition]);   
                    }
                } // FOR
                profiler.concurrent_dtxn.put(profiler.concurrent_dtxn_ids.size());
            }
            
        } // WHILE
    }
    
    /**
     * Reject any and all transactions that are in our queues!
     */
    public synchronized void clearQueues() {
        AbstractTransaction ts = null;
        
        // LOCK QUEUES
        for (int partition : this.localPartitions.values()) {
            if (d) LOG.debug("Clearing out lock queue for partition " + partition);
            synchronized (this.lockQueues[partition]) {
                while ((ts = this.lockQueues[partition].poll()) != null) {
                    this.rejectTransaction(ts, Status.ABORT_REJECT, partition,
                                           this.lockQueuesLastTxn[partition]);
                } // WHILE
            } // SYNCH
        }
        
        // INIT QUEUE
        while ((ts = this.initQueue.poll()) != null) {
            TransactionInitCallback callback = ((LocalTransaction)ts).initTransactionInitCallback();
            callback.abort(Status.ABORT_REJECT);
        } // WHILE
        
        // BLOCKED QUEUE
        while ((ts = this.blockedQueue.poll()) != null) {
            this.blockedQueueTransactions.remove(ts);
            hstore_site.transactionReject((LocalTransaction)ts, Status.ABORT_REJECT);
        } // WHILE
        
        // RESTART QUEUE
        Pair<LocalTransaction, Status> pair = null;
        while ((pair = this.restartQueue.poll()) != null) {
            hstore_site.transactionReject(pair.getFirst(), Status.ABORT_REJECT);
        } // WHILE
    }
    
    // ----------------------------------------------------------------------------
    // INIT QUEUES
    // ----------------------------------------------------------------------------
    
    /**
     * Check whether there are any transactions that need to be released for execution
     * at the partitions controlled by this queue manager
     * Returns true if we released a transaction at at least one partition
     */
    protected boolean checkLockQueues() {
        EstTimeUpdater.update(System.currentTimeMillis());
        
        if (t) LOG.trace("Checking initQueues for " + this.localPartitions.size() + " partitions");
        boolean txn_released = false;
        for (int partition : this.localPartitions.values()) {
            TransactionInitQueueCallback callback = null;
            AbstractTransaction next = null;
            int counter = -1;
            
            if (this.lockQueuesBlocked[partition]) {
                if (t) LOG.trace(String.format("Partition %d is already executing a transaction. Skipping...", partition));
                continue;
            }

            // Poll the queue and get the next value. We need
            // a lock in case somebody is looking for this txnId to remove
            synchronized (this.lockQueues[partition]) {
                next = this.lockQueues[partition].poll();    
            } // SYNCH
            
            // If null, then there is nothing that is ready to run at this partition,
            // so we'll just skip to the next one
            if (next == null) {
                if (t) LOG.trace(String.format("Partition %d initQueue does not have a transaction ready to run. Skipping... " +
                		         "[queueSize=%d]",
                                 partition, this.lockQueues[partition].size()));
                continue;
            }
            
            callback = next.getTransactionInitQueueCallback();
            assert(callback.isInitialized()) :
                String.format("Uninitialized %s callback for %s [hashCode=%d]",
                              callback.getClass().getSimpleName(), next, callback.hashCode());
            
            // If this callback has already been aborted, then there is nothing we need to
            // do. Somebody else will make sure that this txn is removed from the queue
            // We will always want to return true to keep trying to get the next transaction
            if (callback.isAborted()) {
                if (d) LOG.debug(String.format("The next id for partition %d is %s but its callback is marked as aborted. " +
                		         "[queueSize=%d]",
                                 partition, next, this.lockQueuesLastTxn[partition],
                                 this.lockQueues[partition].size()));
                this.lockQueues[partition].remove(next);
                txn_released = true;
                continue;
            }
            // We don't need to acquire lock here because we know that our partition isn't doing
            // anything at this moment. 
            else if (this.lockQueuesLastTxn[partition].compareTo(next.getTransactionId()) > 0) {
                if (d) LOG.debug(String.format("The next id for partition %d is %s but this is less than the previous txn #%d. Rejecting... " +
                		         "[queueSize=%d]",
                                 partition, next, this.lockQueuesLastTxn[partition],
                                 this.lockQueues[partition].size()));
                this.rejectTransaction(next, Status.ABORT_RESTART, partition, this.lockQueuesLastTxn[partition]);
                continue;
            }

            if (d) LOG.debug(String.format("Good news! Partition %d is ready to execute %s! Invoking initQueue callback!",
                             partition, next));
            this.lockQueuesLastTxn[partition] = next.getTransactionId();
            this.lockQueuesBlocked[partition] = true;
            
            // Send the init request for the specified partition
            try {
                callback.run(partition);
                counter = callback.getCounter();
            } catch (NullPointerException ex) {
                // HACK: Ignore...
                if (d) LOG.warn(String.format("Unexpected error when invoking %s for %s at partition %d",
                                callback.getClass().getSimpleName(), next, partition), ex);
            } catch (Throwable ex) {
                String msg = String.format("Failed to invoke %s for %s at partition %d",
                                           callback.getClass().getSimpleName(), next, partition);
                throw new ServerFaultException(msg, ex, next.getTransactionId());
            }
                
            // remove the callback when this partition is the last one to start the job
            if (counter == 0) {
                if (d) LOG.debug(String.format("All local partitions needed by %s are ready.", next));
                txn_released = true;
            }
        } // FOR
        
        if (t) LOG.trace("Finished processing lock queues [released=" + txn_released + "]");
        return (txn_released);
    }
    
    /**
     * Add a new transaction to this queue manager.
     * Returns true if the transaction was successfully inserted at all partitions
     * @param ts
     * @param partitions
     * @param callback
     * @param sysproc TODO
     * @return
     */
    public boolean lockQueueInsert(AbstractTransaction ts,
                                   PartitionSet partitions,
                                   RpcCallback<TransactionInitResponse> callback) {
        assert(ts != null) : "Unexpected null transaction";
        if (d) LOG.debug(String.format("Adding %s into lockQueue [partitions=%s]", ts, partitions));
        
        // Wrap the callback around a TransactionInitWrapperCallback that will wait until
        // our HStoreSite gets an acknowledgment from all the ...
        final TransactionInitQueueCallback wrapper = ts.initTransactionInitQueueCallback(callback);
        assert(wrapper.isInitialized());
        
        boolean should_notify = false;
        boolean ret = true;
        Long txn_id = ts.getTransactionId();
        for (int partition : partitions.values()) {
            // We can preemptively check whether this txnId is greater than
            // the largest one that we know about at a partition
            // We don't need to acquire the lock on last_txns at this partition because 
            // all that we care about is that whatever value is in there now is greater than
            // the what the transaction was trying to use.
            if (this.lockQueuesLastTxn[partition].compareTo(txn_id) > 0) {
                if (d) LOG.debug(String.format("The last lockQueue txnId for remote partition is #%d but this " +
                		         "is greater than %s. Rejecting...",
                                 partition, this.lockQueuesLastTxn[partition], ts));
                if (wrapper != null) {
                    this.rejectTransaction(ts,
                                           Status.ABORT_RESTART,
                                           partition,
                                           this.lockQueuesLastTxn[partition]);
                } else {
                    this.rejectLocalTransaction(ts,
                                                partitions,
                                                (TransactionInitCallback)callback,
                                                Status.ABORT_RESTART,
                                                partition);
                }
                ret = false;
                break;
            }
            
            // There's nothing that we need to do for a remote partition
            if (this.hstore_site.isLocalPartition(partition) == false) continue;
            
            // For local partitions, peek ahead in this partition's queue to see whether the 
            // txnId that we're trying to insert is less than the next one that we expect to release 
            TransactionInitPriorityQueue queue = this.lockQueues[partition];
            AbstractTransaction next_safe = queue.noteTransactionRecievedAndReturnLastSeen(ts);
            
            // The next txnId that we're going to try to execute is already greater
            // than this new txnId that we were given! Rejection!
            if (next_safe.compareTo(ts) > 0) {
                if (d) LOG.debug(String.format("The next safe lockQueue txn for partition #%d is %s but this " +
                		         "is greater than our new txn %s. Rejecting...",
                                 partition, next_safe, ts));
                if (wrapper != null) {
                    this.rejectTransaction(ts,
                                           Status.ABORT_RESTART,
                                           partition,
                                           next_safe.getTransactionId());
                } else {
                    this.rejectLocalTransaction(ts,
                                                partitions,
                                                (TransactionInitCallback)callback,
                                                Status.ABORT_RESTART,
                                                partition);
                }
                ret = false;
                break;
            }
            // Our queue is overloaded. We have to reject the txnId!
            else if (queue.offer(ts, ts.isSysProc()) == false) {
                if (d) LOG.debug(String.format("The initQueue for partition #%d is overloaded. " +
                		        "Throttling %s until id is greater than %s " +
                		        "[locked=%s / queueSize=%d]",
                                 partition, ts, next_safe,
                                 this.lockQueuesBlocked[partition], this.lockQueues[partition].size()));
                if (wrapper != null) {
                    this.rejectTransaction(ts,
                                           Status.ABORT_REJECT,
                                           partition,
                                           next_safe.getTransactionId());
                } else {
                    this.rejectLocalTransaction(ts,
                                                partitions,
                                                (TransactionInitCallback)callback,
                                                Status.ABORT_RESTART,
                                                partition);
                }
                ret = false;
                break;
            }
            // If our queue is currently idle, poke the thread so that it wakes up and tries to
            // schedule our boys!
            else if (this.lockQueuesBlocked[partition] == false) {
                should_notify = true;
            }
            
            if (d) LOG.debug(String.format("Added %s to initQueue for partition %d [locked=%s / queueSize=%d]",
                             ts, partition,
                             this.lockQueuesBlocked[partition], this.lockQueues[partition].size()));
        } // FOR
        if (should_notify && this.checkFlag.availablePermits() == 0)
            this.checkFlag.release();
        return (ret);
    }
    
    /**
     * Mark the transaction as being finished with the given local partition. This can be called
     * either before or after the transaction was initialized at all partitions.
     * @param ts
     * @param status
     * @param partition
     */
    public void lockQueueFinished(AbstractTransaction ts, Status status, int partition) {
        assert(ts.isInitialized()) :
            String.format("Unexpected uninitialized transaction %s [status=%s / partition=%d]", ts, status, partition);
        assert(this.hstore_site.isLocalPartition(partition)) :
            "Trying to mark txn #" + ts + " as finished on remote partition #" + partition;
        if (d) LOG.debug(String.format("Updating lock queues because %s is finished on partition %d [status=%s]",
        		         ts, partition, status));
        
        // If the given txnId is the current transaction at this partition and still holds
        // the lock on the partition, then we will want to release it
        // Note that this is always thread-safe because we will release the lock
        // only if we are the current transaction at this partition
        boolean checkQueue = true;
        if (this.lockQueuesBlocked[partition] && this.lockQueuesLastTxn[partition].equals(ts.getTransactionId())) {
            if (d) LOG.debug(String.format("Unlocking partition %d because %s is finished " +
            		         "[status=%s]",
                             partition, ts, status));
            this.lockQueuesBlocked[partition] = false;
            checkQueue = false;
        } else if (d) {
            LOG.debug(String.format("Not unlocking partition %d for txn %s " +
            		  "[current=%d / locked=%s / status=%s]",
                      partition, ts,
                      this.lockQueuesLastTxn[partition], this.lockQueuesBlocked[partition], status));
        }
        
        // Always attempt to remove it from this partition's queue
        // If this remove() returns false, then we know that our transaction wasn't
        // sitting in the queue for that partition.
        boolean removed = false;
        if (checkQueue) {
            synchronized (this.lockQueues[partition]) {
                removed = this.lockQueues[partition].remove(ts);
            } // SYNCH
        }
        assert(this.lockQueues[partition].contains(ts) == false);
        
        // This is a local transaction that is still waiting for this partition (i.e., it hasn't
        // been rejected yet). That means we will want to decrement the counter its Transaction
        if (removed && status != Status.OK) {
            if (d) LOG.debug(String.format("Removed %s from partition %d queue", ts, partition));
            TransactionInitQueueCallback callback = ts.getTransactionInitQueueCallback();
            try {
                if (callback.isAborted() == false) callback.abort(status);
            } catch (Throwable ex) {
                // XXX
                if (d) {
                    String msg = String.format("Unexpected error when trying to abort %s on partition %d [status=%s]",
                                               ts, partition, status);
                    LOG.warn(msg, ex);
                }
            }
        }
        this.checkFlag.release();
    }

    /**
     * Get the last transaction id that was initialized at the given partition
     */
    public Long getLastLockTransaction(int partition) {
        return (this.lockQueuesLastTxn[partition]);
    }
    
    // ----------------------------------------------------------------------------
    // INTERNAL METHODS
    // ----------------------------------------------------------------------------
    
    /**
     * Reject the given transaction at this QueueManager.
     * @param ts
     * @param callback
     * @param status
     * @param reject_partition
     * @param reject_txnId
     */
    private void rejectLocalTransaction(AbstractTransaction ts,
                                        PartitionSet partitions,
                                        TransactionInitCallback callback,
                                        Status status,
                                        int reject_partition) {
        assert(ts.isInitialized()) :
            String.format("Unexpected uninitalized transaction handle %s in %s [status=%s / rejectPartition]",
                          ts, status, reject_partition);
        if (d) LOG.debug(String.format("Rejecting txn %s on partition %d [status=%s]", ts, status));

        // First send back an ABORT message to the initiating HStoreSite (if we haven't already)
        if (callback.isAborted() == false && callback.isUnblocked() == false) {
            callback.abort(status);
            for (Integer partition : partitions) {
                if (this.hstore_site.isLocalPartition(partition.intValue())) {
                    callback.decrementCounter(1);    
                }
            } // FOR
        }
    }
    
    /**
     * Reject the given transaction at this QueueManager.
     * @param ts
     * @param callback
     * @param status
     * @param reject_partition
     * @param reject_txnId
     */
    private void rejectTransaction(AbstractTransaction ts,
                                   Status status,
                                   int reject_partition,
                                   Long reject_txnId) {
        assert(ts.isInitialized()) :
            String.format("Unexpected uninitalized transaction handle %s in %s [status=%s / rejectPartition]",
                          ts, status, reject_partition);
        if (d) LOG.debug(String.format("Rejecting txn %s on partition %d. Blocking until a txnId greater than #%d [valid=%s]",
                         ts, reject_partition, reject_txnId, (ts.getTransactionId().compareTo(reject_txnId) > 0)));
        assert(ts.getTransactionId().equals(reject_txnId) == false) :
            String.format("Rejected txn %d's blocked-until-id is also %d", ts, reject_txnId); 
        
        TransactionInitQueueCallback callback = ts.getTransactionInitQueueCallback();
        
        // First send back an ABORT message to the initiating HStoreSite (if we haven't already)
        if (callback.isAborted() == false && callback.isUnblocked() == false) {
            try {
                callback.abort(status, reject_partition, reject_txnId);
            } catch (Throwable ex) {
                // XXX
                if (d) {
                    String msg = String.format("Unexpected error when trying to abort txn #%d " +
                    		                   "[status=%s, rejectPartition=%d, rejectTxnId=%s]",
                                               ts, status, reject_partition, reject_txnId);
                    LOG.warn(msg, ex); 
                    // throw new RuntimeException(msg, ex);
                }
            }
        }
        
        // Then make sure that our txnId is removed from all of the local partitions
        // that we queued it on.
        boolean poke = false;
        for (int partition : callback.getPartitions().values()) {
            if (this.localPartitions.contains(partition) == false) continue;
            
            // Try to remove it from our queue. If we can't then it might
            // be that we're the current transaction at this partition, so that
            // we need to make sure that we release the locks
            boolean removed = false;
            synchronized (this.lockQueues[partition]) {
                removed = this.lockQueues[partition].remove(ts);
            } // SYNCH
            
            // We don't need to acquire a lock here because we know that
            // nobody else can update us unless the lock flag is false
            if (removed == false && this.lockQueuesBlocked[partition] && 
                    this.lockQueuesLastTxn[partition].equals(ts)) {
                this.lockQueuesBlocked[partition] = false;
                poke = true;
            }
        } // FOR
        if (poke && this.checkFlag.availablePermits() == 0)
            this.checkFlag.release();
    }

    // ----------------------------------------------------------------------------
    // TRANSACTION INIT
    // ----------------------------------------------------------------------------
    
    public void initTransaction(LocalTransaction ts) {
        Long txn_id = ts.getTransactionId();
        
        // Check whether our transaction can't run right now because its id is less than
        // the last seen txnid from the remote partitions that it wants to touch
        for (Integer partition : ts.getPredictTouchedPartitions()) {
            Long last_txn_id = this.lockQueuesLastTxn[partition.intValue()]; 
            if (txn_id.compareTo(last_txn_id) < 0) {
                // If we catch it here, then we can just block ourselves until
                // we generate a txn_id with a greater value and then re-add ourselves
                if (d) {
                    LOG.warn(String.format("%s - Unable to queue transaction because the last txn " +
                             "id at partition %d is %d. Restarting...",
                             ts, partition, last_txn_id));
                    LOG.warn(String.format("LastTxnId:#%s / NewTxnId:#%s",
                             TransactionIdManager.toString(last_txn_id),
                             TransactionIdManager.toString(txn_id)));
                }
                if (hstore_conf.site.txn_counters && ts.getRestartCounter() == 1) {
                    TransactionCounter.BLOCKED_LOCAL.inc(ts.getProcedure());
                }
                this.blockTransaction(ts, partition.intValue(), last_txn_id);
                return;
            }
        } // FOR

        this.initQueue.add(ts);
        if (this.checkFlag.availablePermits() == 0)
            this.checkFlag.release();
    }
    
    private void checkInitQueue() {
        LocalTransaction ts = null;
        HStoreCoordinator hstore_coordinator = hstore_site.getCoordinator();
        while ((ts = this.initQueue.poll()) != null) {
            TransactionInitCallback callback = ts.initTransactionInitCallback();
            hstore_coordinator.transactionInit(ts, callback);
        } // WHILE
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
        synchronized (this.lockQueuesLastTxn[partition]) {
            if (this.lockQueuesLastTxn[partition].compareTo(txn_id) < 0) {
                if (d) LOG.debug(String.format("Marking txn #%d as last txnId for remote partition %d", txn_id, partition));
                this.lockQueuesLastTxn[partition] = txn_id;
            }
        } // SYNCH
    }
    
    /**
     * A LocalTransaction from this HStoreSite is blocked because a remote HStoreSite that it needs to
     * access a partition on has its last tranasction id as greater than what the LocalTransaction was issued.
     * @param ts
     * @param partition
     * @param last_txnId
     */
    public void blockTransaction(LocalTransaction ts, int partition, Long last_txnId) {
        if (d) LOG.debug(String.format("%s - Blocking transaction until after a txnId greater than %d " +
                         "is created for partition %d",
                         ts, last_txnId, partition));
       
        // IMPORTANT: Mark this transaction as not being deletable.
        //            This will prevent it from getting deleted out from under us
        ts.markNeedsRestart();
        
        if (this.blockedQueueTransactions.putIfAbsent(ts, last_txnId) != null) {
            Long other_txn_id = this.blockedQueueTransactions.get(ts);
            if (other_txn_id != null && other_txn_id.compareTo(last_txnId) < 0) {
                this.blockedQueueTransactions.put(ts, last_txnId);
            }
        } else {
            this.blockedQueue.offer(ts);
        }
        if (hstore_site.isLocalPartition(partition) == false) {
            this.markLastTransaction(partition, last_txnId);
        }
        if (hstore_conf.site.txn_counters && ts.getRestartCounter() == 1) {
            TransactionCounter.BLOCKED_REMOTE.inc(ts.getProcedure());
            int id = (int)TransactionIdManager.getInitiatorIdFromTransactionId(last_txnId.longValue());
            synchronized (this.blockedQueueHistogram) {
                this.blockedQueueHistogram.put(id);
            } // SYNCH
        }
        if (this.checkFlag.availablePermits() == 0)
            this.checkFlag.release();
    }
    
    /**
     * 
     * @param last_txn_id
     */
    private void checkBlockedQueue() {
        if (t) LOG.trace(String.format("Checking whether we can release %d blocked dtxns",
                                       this.blockedQueue.size()));
        
        while (this.blockedQueue.isEmpty() == false) {
            LocalTransaction ts = this.blockedQueue.peek();
            Long releaseTxnId = this.blockedQueueTransactions.get(ts);
            if (releaseTxnId == null) {
                if (d) LOG.warn("Missing release TxnId for " + ts);
                try {
                    this.blockedQueue.remove(ts);
                } catch (NullPointerException ex) {
                    // XXX: IGNORE
                }
                continue;
            }
            
            // Check whether the last txnId issued by the TransactionIdManager at the transactions'
            // base partition is greater than the one that we can be released on 
            TransactionIdManager txnIdManager = hstore_site.getTransactionIdManager(ts.getBasePartition());
            Long last_txn_id = txnIdManager.getLastTxnId();
            if (releaseTxnId.compareTo(last_txn_id) < 0) {
                if (d) LOG.debug(String.format("Releasing blocked %s because the lastest txnId was #%d [release=%d]",
                                               ts, last_txn_id, releaseTxnId));
                this.blockedQueue.remove();
                this.blockedQueueTransactions.remove(ts);
                this.hstore_site.transactionRestart(ts, Status.ABORT_RESTART);
                ts.unmarkNeedsRestart();
                this.hstore_site.queueDeleteTransaction(ts.getTransactionId(), Status.ABORT_REJECT);
            // For now we can break, but I think that we may need separate
            // queues for the different partitions...
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
        ts.markNeedsRestart();
        
        if (this.restartQueue.offer(Pair.of(ts, status)) == false) {
            if (d) LOG.debug(String.format("%s - Unable to add txn to restart queue. Rejecting...", ts));
            this.hstore_site.transactionReject(ts, Status.ABORT_REJECT);
            ts.unmarkNeedsRestart();
            this.hstore_site.queueDeleteTransaction(ts.getTransactionId(), Status.ABORT_REJECT);
            return;
        }
        if (d) LOG.debug(String.format("%s - Successfully added txn to restart queue.", ts));
        if (this.checkFlag.availablePermits() == 0)
            this.checkFlag.release();
    }
    
    private void checkRestartQueue() {
        Pair<LocalTransaction, Status> pair = null;
        while ((pair = this.restartQueue.poll()) != null) {
            LocalTransaction ts = pair.getFirst();
            Status status = pair.getSecond();
            
            if (d) LOG.debug(String.format("%s - Ready to restart transaction [status=%s]", ts, status));
            Status ret = this.hstore_site.transactionRestart(ts, status);
            if (d) LOG.debug(String.format("%s - Got return result %s after restarting", ts, ret));
            
            ts.unmarkNeedsRestart();
            this.hstore_site.queueDeleteTransaction(ts.getTransactionId(), ret);
        } // WHILE
    }
    
    // ----------------------------------------------------------------------------
    // UTILITY METHODS
    // ----------------------------------------------------------------------------
    
    @Override
    public void updateConf(HStoreConf hstore_conf) {
        // HACK: If there is only one site in the cluster, then we can
        // set the wait time to 1ms
        if (hstore_site.getCatalogContext().numberOfSites == 1) {
            this.wait_time = 1;
        }
        else {
            this.wait_time = hstore_conf.site.txn_incoming_delay;            
        }
    }
    
    public TransactionQueueManagerProfiler getProfiler() {
        return this.profiler;
    }
    
    @Override
    public void prepareShutdown(boolean error) {
        // Nothing for now
        // Probably should abort all queued txns.
        this.clearQueues();
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
        m[++idx] = new LinkedHashMap<String, Object>();
        m[idx].put("Wait Time", this.wait_time + " ms");
        m[idx].put("# of Blocked Txns", this.blockedQueue.size());
        
        // Local Partitions
        m[++idx] = new LinkedHashMap<String, Object>();
        for (int p = 0; p < this.lockQueuesLastTxn.length; p++) {
            Map<String, Object> inner = new LinkedHashMap<String, Object>();
            inner.put("Current Txn", this.lockQueuesLastTxn[p]);
            if (hstore_site.isLocalPartition(p)) {
                inner.put("Locked?", this.lockQueuesBlocked[p]);
                inner.put("Queue Size", this.lockQueues[p].size());
            }
            m[idx].put(String.format("Partition #%02d", p), inner);
        } // FOR
        
        return StringUtil.formatMaps(m);
    }
    
    // ----------------------------------------------------------------------------
    // DEBUG METHODS
    // ----------------------------------------------------------------------------

    public class Debug implements DebugContext {
        public int getInitQueueSize() {
            int ctr = 0;
            for (int p : localPartitions.values())
              ctr += lockQueues[p].size();
            return (ctr);
        }
        public int getInitQueueSize(int partition) {
            return (lockQueues[partition].size());
        }
        public TransactionInitPriorityQueue getInitQueue(int partition) {
            return (lockQueues[partition]);
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
        public boolean isLockQueuesEmpty() {
            for (int i = 0; i < lockQueues.length; ++i) {
                if (lockQueues[i].isEmpty() == false) return (false);
            }
            return (true);
        }
        /**
         * Return the current transaction that is executing at this partition
         * @param partition
         * @return
         */
        public Long getCurrentTransaction(int partition) {
            if (lockQueuesBlocked[partition]) {
                return (lockQueuesLastTxn[partition]);
            }
            return (null);
        }
    }
    
    private TransactionQueueManager.Debug cachedDebugContext;
    public TransactionQueueManager.Debug getDebugContext() {
        if (cachedDebugContext == null) {
            // We don't care if we're thread-safe here...
            cachedDebugContext = new TransactionQueueManager.Debug();
        }
        return cachedDebugContext;
    }
}
