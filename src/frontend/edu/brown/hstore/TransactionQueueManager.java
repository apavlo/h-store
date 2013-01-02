package edu.brown.hstore;

import java.util.Comparator;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;
import org.voltdb.CatalogContext;
import org.voltdb.TransactionIdManager;
import org.voltdb.exceptions.ServerFaultException;
import org.voltdb.utils.Pair;

import edu.brown.hstore.Hstoreservice.Status;
import edu.brown.hstore.callbacks.PartitionCountingCallback;
import edu.brown.hstore.conf.HStoreConf;
import edu.brown.hstore.txns.AbstractTransaction;
import edu.brown.hstore.txns.LocalTransaction;
import edu.brown.hstore.util.TransactionCounter;
import edu.brown.interfaces.Configurable;
import edu.brown.interfaces.DebugContext;
import edu.brown.interfaces.Shutdownable;
import edu.brown.logging.LoggerUtil;
import edu.brown.logging.LoggerUtil.LoggerBoolean;
import edu.brown.profilers.TransactionQueueManagerProfiler;
import edu.brown.statistics.ObjectHistogram;
import edu.brown.utils.PartitionSet;
import edu.brown.utils.StringUtil;

/**
 * 
 * @author pavlo
 */
public class TransactionQueueManager implements Runnable, Shutdownable, Configurable {
    private static final Logger LOG = Logger.getLogger(TransactionQueueManager.class);
    private static final LoggerBoolean debug = new LoggerBoolean(LOG.isDebugEnabled());
    private static final LoggerBoolean trace = new LoggerBoolean(LOG.isTraceEnabled());
    static {
        LoggerUtil.attachObserver(LOG, debug, trace);
    }
    
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
    private int wait_time;
    
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
     * The txnId that currently has the lock for each partition
     */
    private final boolean[] lockQueuesBlocked;
    
    private final TransactionQueueManagerProfiler[] profilers;

    
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
    private final ObjectHistogram<Integer> blockedQueueHistogram = new ObjectHistogram<Integer>();
    
    // ----------------------------------------------------------------------------
    // TRANSACTIONS THAT NEED TO INIT
    // ----------------------------------------------------------------------------

    /**
     * A queue of transactions that need to be added to the lock queues at the partitions 
     * at this site.
     */
    private final Queue<AbstractTransaction> initQueues[];// = new ConcurrentLinkedQueue<AbstractTransaction>(); 
    
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
    @SuppressWarnings("unchecked")
    public TransactionQueueManager(HStoreSite hstore_site) {
        CatalogContext catalogContext = hstore_site.getCatalogContext();
        PartitionSet allPartitions = catalogContext.getAllPartitionIds();
        this.localPartitions = hstore_site.getLocalPartitionIds();
        int num_partitions = allPartitions.size();
        
        this.hstore_site = hstore_site;
        this.hstore_conf = hstore_site.getHStoreConf();
        this.lockQueues = new TransactionInitPriorityQueue[num_partitions];
        this.lockQueuesBlocked = new boolean[this.lockQueues.length];
        this.lockQueuesLastTxn = new Long[this.lockQueues.length];
        this.initQueues = new Queue[num_partitions];
        this.profilers = new TransactionQueueManagerProfiler[num_partitions];
        
        this.updateConf(this.hstore_conf);
        
        // Allocate transaction queues
        for (int partition : allPartitions.values()) {
            this.lockQueuesLastTxn[partition] = Long.valueOf(-1);
            if (this.hstore_site.isLocalPartition(partition)) {
                this.lockQueues[partition] = new TransactionInitPriorityQueue(partition, this.wait_time);
                this.lockQueuesBlocked[partition] = false;
                this.initQueues[partition] = new ConcurrentLinkedQueue<AbstractTransaction>();
                this.profilers[partition] = new TransactionQueueManagerProfiler(num_partitions);
            }
        } // FOR
        
        if (debug.val) LOG.debug(String.format("Created %d TransactionInitQueues for %s",
                                 num_partitions, hstore_site.getSiteName()));
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
        
        if (debug.val) LOG.debug("Starting distributed transaction queue manager thread");
        
        while (this.stop == false) {
            // if (hstore_conf.site.queue_profiling) profiler.idle.start();
            try {
                this.checkFlag.tryAcquire(this.wait_time, TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
                // Nothing...
            } finally {
                // if (hstore_conf.site.queue_profiling && profiler.idle.isStarted()) profiler.idle.stop();
            }
            
//            if (trace.val) LOG.trace("Checking partition queues for dtxns to release!");
//            if (hstore_conf.site.queue_profiling) profiler.lock_queue.start();
//            while (this.checkLockQueues()) {
//                // Keep checking the queue as long as they have more stuff in there
//                // for us to process
//            }
//            if (hstore_conf.site.queue_profiling && profiler.lock_queue.isStarted()) profiler.lock_queue.stop();
            
            // Release transactions for initialization
            for (int partition : this.localPartitions.values()) {
                this.checkInitQueue(partition);
//                int added = this.checkInitQueue(partition);
//                if (added == 0) {
//                    QueueState state = this.lockQueues[partition].checkQueueState();
//                    if (trace.val) LOG.trace(String.format("Partition %d :: State=%s", partition, state));
//                }
            } // FOR
            
            // Release blocked distributed transactions
//            if (hstore_conf.site.queue_profiling) profiler.block_queue.start();
            this.checkBlockedQueue();
//            if (hstore_conf.site.queue_profiling && profiler.block_queue.isStarted()) profiler.block_queue.stop();
            
            // Requeue mispredicted local transactions
//            if (hstore_conf.site.queue_profiling) profiler.restart_queue.start();
            this.checkRestartQueue();
//            if (hstore_conf.site.queue_profiling && profiler.restart_queue.isStarted()) profiler.restart_queue.stop();
            
            // Count the number of unique concurrent dtxns
//            if (hstore_conf.site.queue_profiling) {
//                profiler.concurrent_dtxn_ids.clear();
//                for (int partition: this.localPartitions.values()) {
//                    if (this.lockQueuesLastTxn[partition].longValue() >= 0) {
//                        profiler.concurrent_dtxn_ids.add(this.lockQueuesLastTxn[partition]);   
//                    }
//                } // FOR
//                profiler.concurrent_dtxn.put(profiler.concurrent_dtxn_ids.size());
//            }
            
        } // WHILE
    }
    
    /**
     * Reject any and all transactions that are in our queues!
     */
    public void clearQueues(int partition) {
        AbstractTransaction ts = null;
        // Long txnId = null;
        
        
        if (debug.val) LOG.debug("Clearing out lock queue for partition " + partition);
        // LOCK QUEUES
        synchronized (this.lockQueues[partition]) {
            while ((ts = this.lockQueues[partition].poll()) != null) {
                this.rejectTransaction(ts,
                                       Status.ABORT_REJECT,
                                       partition,
                                       this.lockQueuesLastTxn[partition]);
            } // WHILE
        } // SYNCH
        
        // INIT QUEUE
//        while ((ts = this.initQueues[partition].poll()) != null) {
//            TransactionInitQueueCallback callback = ts.getTransactionInitQueueCallback();
//            callback.abort(Status.ABORT_REJECT);
//        } // WHILE
        
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

    protected int checkInitQueue(int partition) {
        if (hstore_conf.site.queue_profiling) profilers[partition].init_queue.start();
        // Process initialization queue
        AbstractTransaction next_init = null;
        int added = 0;
        while ((next_init = this.initQueues[partition].poll()) != null) {
            PartitionCountingCallback<AbstractTransaction> callback = next_init.getTransactionInitQueueCallback();
            if (this.lockQueueInsert(next_init, partition, callback)) {
                added++;
            }
        } // WHILE
        if (hstore_conf.site.queue_profiling) profilers[partition].init_queue.stop();
        return (added);
    }
    
    protected void initTransaction(AbstractTransaction ts) {
        for (int partition : ts.getPredictTouchedPartitions().values()) {
            if (this.localPartitions.contains(partition)) {
                this.initQueues[partition].add(ts);      
            }
        } // FOR
        if (this.checkFlag.availablePermits() == 0)
            this.checkFlag.release();
    }
    
    
    /**
     * Check whether there are any transactions that need to be released for execution
     * at the partitions controlled by this queue manager
     * Returns true if we released a transaction at at least one partition
     */
    protected AbstractTransaction checkLockQueue(int partition) {
        if (hstore_conf.site.queue_profiling) profilers[partition].lock_queue.start();
        if (trace.val) LOG.trace(String.format("Checking lock queue for partition %d [queueSize=%d]",
                                 partition, this.lockQueues[partition].size()));
        
        if (this.lockQueuesBlocked[partition] != false) {
            if (trace.val) LOG.warn(String.format("Partition %d is already executing transaction %d. Skipping...",
                                    partition, this.lockQueuesLastTxn[partition]));
            if (hstore_conf.site.queue_profiling) profilers[partition].lock_queue.stop();
            return (null);
        }
        
        if (trace.val) LOG.trace("Checking initQueues for " + this.localPartitions.size() + " partitions");
        PartitionCountingCallback<AbstractTransaction> callback = null;
        AbstractTransaction nextTxn = null;
        // Long nextTxnId = null;
        while (nextTxn == null) {
            // Poll the queue and get the next value. 
            nextTxn = this.lockQueues[partition].poll();

            // If null, then there is nothing that is ready to run at this partition,
            // so we'll just skip to the next one
            if (nextTxn == null) {
                if (trace.val) LOG.trace(String.format("Partition %d initQueue does not have a transaction ready to run. Skipping... " +
                		         "[queueSize=%d]",
                                 partition, this.lockQueues[partition].size()));
                this.lockQueues[partition].checkQueueState();
                break;
            }
            
            callback = nextTxn.getTransactionInitQueueCallback();
            assert(callback.isInitialized()) :
                String.format("Uninitialized %s callback for %s [hashCode=%d]",
                              callback.getClass().getSimpleName(), nextTxn, callback.hashCode());
            
            // If this callback has already been aborted, then there is nothing we need to
            // do. Somebody else will make sure that this txn is removed from the queue
            if (callback.isAborted()) {
                if (debug.val)
                    LOG.debug(String.format("The next id for partition %d is %s but its callback is marked as aborted. " +
                              "[queueSize=%d]",
                              partition, nextTxn, this.lockQueuesLastTxn[partition],
                              this.lockQueues[partition].size()));
                this.lockQueues[partition].remove(nextTxn);
                nextTxn = null;
                // Repeat so that we can try again
                continue;
            }
            // We don't need to acquire lock here because we know that our partition isn't doing
            // anything at this moment. 
            else if (this.lockQueuesLastTxn[partition].compareTo(nextTxn.getTransactionId()) > 0) {
                if (debug.val)
                    LOG.debug(String.format("The next id for partition %d is %s but this is less than the previous txn #%d. Rejecting... " +
                              "[queueSize=%d]",
                              partition, nextTxn, this.lockQueuesLastTxn[partition],
                              this.lockQueues[partition].size()));
                if (hstore_conf.site.queue_profiling) profilers[partition].lock_queue.stop();
                this.rejectTransaction(nextTxn,
                                       Status.ABORT_RESTART,
                                       partition,
                                       this.lockQueuesLastTxn[partition]);
                nextTxn = null;
                continue;
            }

            if (debug.val)
                LOG.debug(String.format("Good news! Partition %d is ready to execute %s! Invoking initQueue callback!",
                          partition, nextTxn));
            this.lockQueuesLastTxn[partition] = nextTxn.getTransactionId();
            this.lockQueuesBlocked[partition] = true; 
            
            // Send the init request for the specified partition
            try {
                callback.run(partition);
            } catch (NullPointerException ex) {
                // HACK: Ignore...
                if (debug.val) LOG.warn(String.format("Unexpected error when invoking %s for %s at partition %d",
                                callback.getClass().getSimpleName(), nextTxn, partition), ex);
            } catch (Throwable ex) {
                String msg = String.format("Failed to invoke %s for %s at partition %d",
                                           callback.getClass().getSimpleName(), nextTxn, partition);
                throw new ServerFaultException(msg, ex, nextTxn.getTransactionId());
            }
        } // WHILE
        
        if (debug.val && nextTxn != null && nextTxn.isPredictSinglePartition() == false)
            LOG.debug(String.format("Finished processing lock queue for partition %d [next=%s]",
                      partition, nextTxn));
        if (hstore_conf.site.queue_profiling) profilers[partition].lock_queue.stop();
        return (nextTxn);
    }
    
    /**
     * Add a new transaction to this queue manager.
     * Returns true if the transaction was successfully inserted at all partitions
     * <B>Note:</B> This should not be called directly. You probably want to use initTransaction().
     * @param ts
     * @param partitions
     * @param callback
     * @return
     */
    protected boolean lockQueueInsert(AbstractTransaction ts,
                                      int partition,
                                      PartitionCountingCallback<? extends AbstractTransaction> callback) {
        assert(ts.isInitialized()) :
            String.format("Unexpected uninitialized transaction %s [partition=%d]", ts, partition);
        assert(this.hstore_site.isLocalPartition(partition)) :
            String.format("Trying to add %s to non-local partition %d", ts, partition);

        // This is actually bad and should never happen. But for the sake of trying
        // to get the experiments working, we're just going to ignore it...
        if (callback.isInitialized() == false) {
            LOG.warn(String.format("Unexpected uninitialized %s for %s [partition=%d]", callback.getClass().getSimpleName(), ts, partition));
            return (false);
        }
        
        if (debug.val)
            LOG.debug(String.format("Adding %s into lockQueue for partition %d [allPartitions=%s]",
                      ts, partition, ts.getPredictTouchedPartitions()));
        
        // We can preemptively check whether this txnId is greater than
        // the largest one that we know about at a partition
        // We don't need to acquire the lock on last_txns at this partition because 
        // all that we care about is that whatever value is in there now is greater than
        // the what the transaction was trying to use.
        Long txn_id = ts.getTransactionId();
        if (this.lockQueuesLastTxn[partition].compareTo(txn_id) > 0) {
            if (debug.val)
                LOG.debug(String.format("The last lockQueue txnId for remote partition is #%d but this " +
            	          "is greater than %s. Rejecting...",
                          partition, this.lockQueuesLastTxn[partition], ts));
            this.rejectTransaction(ts,
                                   Status.ABORT_RESTART,
                                   partition,
                                   this.lockQueuesLastTxn[partition]);
            return (false);
        }
        
        // For local partitions, peek ahead in this partition's queue to see whether the 
        // txnId that we're trying to insert is less than the next one that we expect to release 
        //
        // 2012-12-03 - There is a race condition here where we may get back the last txn that 
        // was released but then it was deleted and cleaned-up. This means that its txn id
        // might be null. A better way to do this is to only have each PartitionExecutor
        // insert the new transaction into its queue. 
        Long next_safe_id = this.lockQueues[partition].noteTransactionRecievedAndReturnLastSeen(txn_id);
        
        // The next txnId that we're going to try to execute is already greater
        // than this new txnId that we were given! Rejection!
        if (this.lockQueuesLastTxn[partition].compareTo(txn_id) > 0) {
            if (debug.val)
                LOG.debug(String.format("The next safe lockQueue txn for partition #%d is %s but this " +
            	          "is greater than our new txn %s. Rejecting...",
                          partition, this.lockQueuesLastTxn[partition], ts));
            this.rejectTransaction(ts,
                                   Status.ABORT_RESTART,
                                   partition,
                                   next_safe_id);
            return (false);
        }
        // Our queue is overloaded. We have to reject the txnId!
        else if (this.lockQueues[partition].offer(ts) == false) {
            if (debug.val)
                LOG.debug(String.format("The initQueue for partition #%d is overloaded. " +
            	          "Throttling %s until id is greater than %s " +
            		      "[locked=%s, queueSize=%d]",
                          partition, ts, next_safe_id,
                          this.lockQueuesBlocked[partition], this.lockQueues[partition].size()));
            this.rejectTransaction(ts,
                                   Status.ABORT_REJECT,
                                   partition,
                                   next_safe_id);
            return (false);
        }
        if (trace.val)
            LOG.trace(String.format("Added %s to initQueue for partition %d [locked=%s, queueSize=%d]",
                      ts, partition,
                      this.lockQueuesBlocked[partition], this.lockQueues[partition].size()));
        return (true);
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
            String.format("Unexpected uninitialized transaction %s [status=%s, partition=%d]", ts, status, partition);
        assert(this.hstore_site.isLocalPartition(partition)) :
            "Trying to mark txn #" + ts + " as finished on remote partition #" + partition;
        if (debug.val)
            LOG.debug(String.format("%s is finished on partition %d. Checking whether to update queues " +
        	          "[status=%s]",
        		      ts, partition, status));
        
        // If the given txnId is the current transaction at this partition and still holds
        // the lock on the partition, then we will want to release it
        // Note that this is always thread-safe because we will release the lock
        // only if we are the current transaction at this partition
        boolean checkQueue = true;
        if (this.lockQueuesBlocked[partition] != false &&
            this.lockQueuesLastTxn[partition].equals(ts.getTransactionId())) {
            if (debug.val)
                LOG.debug(String.format("Unlocking partition %d because %s is finished " +
            	          "[status=%s]",
                          partition, ts, status));
            this.lockQueuesBlocked[partition] = false;
            checkQueue = false;
        }
        // If it wasn't running, then we need to make sure that we remove it from
        // our initialization queue. Unfortunately this means that we need to traverse
        // the queue to find it and remove.
        // FIXME: Need to think of a better way of doing this or to not have the 
        // the PartitionExecutor's thread have to do this
        else {
//            boolean result = this.initQueues[partition].remove(ts);
//            if (d && result)
//                LOG.debug(String.format("Removed %s from partition %d initialization queue " +
//                          "[status=%s]",
//                          ts, partition, status));
//            checkQueue = (result == false);
        }
        
        // Always attempt to remove it from this partition's queue
        // If this remove() returns false, then we know that our transaction wasn't
        // sitting in the queue for that partition.
        boolean removed = false;
        if (checkQueue) {
            removed = this.lockQueues[partition].remove(ts);
        }
        assert(this.lockQueues[partition].contains(ts) == false) :
            String.format("The %s for partition %d contains %s even though it should not! " +
            		      "[checkQueue=%s, removed=%s]",
            		      this.lockQueues[partition].getClass().getSimpleName(), partition,
            		      checkQueue, removed);
        
        // This is a local transaction that is still waiting for this partition (i.e., it hasn't
        // been rejected yet). That means we will want to decrement the counter its Transaction
        if (removed && status != Status.OK) {
            if (debug.val) LOG.debug(String.format("Removed %s from partition %d queue", ts, partition));
            PartitionCountingCallback<AbstractTransaction> callback = ts.getTransactionInitQueueCallback();
            try {
                if (callback.isAborted() == false) callback.abort(status);
            } catch (Throwable ex) {
                String msg = String.format("Unexpected error when trying to abort %s on partition %d [status=%s]",
                                           ts, partition, status);
                if (debug.val) LOG.warn(msg, ex);
                throw new RuntimeException(msg, ex);
            }
        }
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
    private void rejectTransaction(AbstractTransaction ts,
                                   Status status,
                                   int reject_partition,
                                   Long reject_txnId) {
        assert(ts.isInitialized()) :
            String.format("Uninitialized transaction handle %s [status=%s, rejectPartition=%d]",
                          ts, status, reject_partition);
        assert(reject_txnId != null) :
            String.format("Null reject txn id for %s [status=%s, rejectPartition=%d]",
                          ts, status, reject_partition);
        if (debug.val) {
            Long txnId = ts.getTransactionId();
            boolean is_valid = (reject_txnId == null || txnId.compareTo(reject_txnId) > 0);
            LOG.debug(String.format("Rejecting %s on partition %d. Blocking until a txnId greater than #%d " +
            		  "[status=%s, valid=%s]",
                      ts, reject_partition, reject_txnId, status, is_valid));
        }
        
        PartitionCountingCallback<AbstractTransaction> callback = ts.getTransactionInitQueueCallback();
        
        // First send back an ABORT message to the initiating HStoreSite (if we haven't already)
        if (callback.isAborted() == false && callback.isUnblocked() == false) {
            reject_txnId = Long.valueOf(reject_txnId.longValue() + 5); // HACK
            assert(ts.getTransactionId().equals(reject_txnId) == false) :
                String.format("Aborted txn %s's rejection txnId is also %d [status=%s]", ts, reject_txnId, status);
            try {
                callback.abort(status);
//                if (status == Status.ABORT_RESTART || status == Status.ABORT_REJECT) {
//                    callback.abort(status, reject_partition, reject_txnId);    
//                } else {
//                    callback.abort(status);
//                }
            } catch (Throwable ex) {
                String msg = String.format("Unexpected error when trying to abort txn %s " +
                                           "[status=%s, rejectPartition=%d, rejectTxnId=%s]",
                                           ts, status, reject_partition, reject_txnId);
                if (debug.val) LOG.warn(msg, ex); 
                throw new RuntimeException(msg, ex);
            }
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
    public void markLastTransaction(int partition, Long txn_id) {
        assert(this.hstore_site.isLocalPartition(partition) == false) :
            "Trying to mark the last seen txnId for local partition #" + partition;
        
        // This lock is low-contention because we don't update the last txnId seen
        // at partitions very often.
        synchronized (this.lockQueuesLastTxn[partition]) {
            if (this.lockQueuesLastTxn[partition].compareTo(txn_id) < 0) {
                if (debug.val) LOG.debug(String.format("Marking txn #%d as last txnId for remote partition %d", txn_id, partition));
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
        if (debug.val) LOG.debug(String.format("%s - Blocking transaction until after a txnId greater than %d " +
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
        if (trace.val) LOG.trace(String.format("Checking whether we can release %d blocked dtxns",
                                 this.blockedQueue.size()));
        
        while (this.blockedQueue.isEmpty() == false) {
            LocalTransaction ts = this.blockedQueue.peek();
            Long releaseTxnId = this.blockedQueueTransactions.get(ts);
            if (releaseTxnId == null) {
                if (debug.val) LOG.warn("Missing release TxnId for " + ts);
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
                if (debug.val) LOG.debug(String.format("Releasing blocked %s because the lastest txnId was #%d [release=%d]",
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
        if (debug.val) LOG.debug(String.format("%s - Requeing transaction for execution [status=%s]", ts, status));
        ts.markNeedsRestart();
        
        if (this.restartQueue.offer(Pair.of(ts, status)) == false) {
            if (debug.val) LOG.debug(String.format("%s - Unable to add txn to restart queue. Rejecting...", ts));
            this.hstore_site.transactionReject(ts, Status.ABORT_REJECT);
            ts.unmarkNeedsRestart();
            this.hstore_site.queueDeleteTransaction(ts.getTransactionId(), Status.ABORT_REJECT);
            return;
        }
        if (debug.val) LOG.debug(String.format("%s - Successfully added txn to restart queue.", ts));
        if (this.checkFlag.availablePermits() == 0)
            this.checkFlag.release();
    }
    
    private void checkRestartQueue() {
        Pair<LocalTransaction, Status> pair = null;
        while ((pair = this.restartQueue.poll()) != null) {
            LocalTransaction ts = pair.getFirst();
            Status status = pair.getSecond();
            
            if (debug.val) LOG.debug(String.format("%s - Ready to restart transaction [status=%s]", ts, status));
            Status ret = this.hstore_site.transactionRestart(ts, status);
            if (debug.val) LOG.debug(String.format("%s - Got return result %s after restarting", ts, ret));
            
            ts.unmarkNeedsRestart();
            this.hstore_site.queueDeleteTransaction(ts.getTransactionId(), status);
        } // WHILE
    }
    
    // ----------------------------------------------------------------------------
    // UTILITY METHODS
    // ----------------------------------------------------------------------------
    
    public TransactionInitPriorityQueue getInitQueue(int partition) {
        return (this.lockQueues[partition]);
    }
    
    @Override
    public void updateConf(HStoreConf hstore_conf) {
        // HACK: If there is only one site in the cluster, then we can
        // set the wait time to 1ms
        if (hstore_site.getCatalogContext().numberOfSites == -1) { // XXX
            this.wait_time = 1;
        }
        else {
            this.wait_time = hstore_conf.site.txn_incoming_delay;            
        }
    }
    
    @Override
    public void prepareShutdown(boolean error) {
        // Nothing for now
        // Probably should abort all queued txns.
        for (int partition : this.localPartitions.values()) {
            this.clearQueues(partition);
        }
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
            Set<AbstractTransaction> allTxns = new HashSet<AbstractTransaction>();
            for (int p : localPartitions.values()) {
                allTxns.addAll(lockQueues[p]);
            }
            return (allTxns.size());
        }
        public int getInitQueueSize(int partition) {
            return (lockQueues[partition].size());
        }
        public int getBlockedQueueSize() {
            return (blockedQueue.size());
        }
        public int getRestartQueueSize() {
            return (restartQueue.size());
        }
        public ObjectHistogram<Integer> getBlockedDtxnHistogram() {
            return (blockedQueueHistogram);
        }
        public TransactionQueueManagerProfiler getProfiler(int partition) {
            return (profilers[partition]);
        }
        /**
         * Returns true if all of the partition's lock queues are empty
         * <b>NOTE:</b> This is not thread-safe.
         * @return
         */
        public boolean isLockQueuesEmpty() {
            for (int i = 0; i < lockQueues.length; ++i) {
                if (lockQueues[i].isEmpty() == false) return (false);
            }
            return (true);
        }
        /**
         * Returns true if the given partition is currently locked.
         * <b>NOTE:</b> This is not thread-safe.
         * @param partition
         * @return
         */
        public boolean isLocked(int partition) {
            return (lockQueuesBlocked[partition]);
        }
        /**
         * Return the current transaction that is executing at this partition
         * <b>NOTE:</b> This is not thread-safe.
         * @param partition
         * @return
         */
        public Long getCurrentTransaction(int partition) {
            return (lockQueuesLastTxn[partition]);
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
