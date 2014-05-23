package edu.brown.hstore;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.ConcurrentModificationException;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.log4j.Logger;
import org.voltdb.CatalogContext;
import org.voltdb.exceptions.ServerFaultException;
import org.voltdb.utils.Pair;

import edu.brown.hstore.Hstoreservice.Status;
import edu.brown.hstore.callbacks.PartitionCountingCallback;
import edu.brown.hstore.conf.HStoreConf;
import edu.brown.hstore.txns.AbstractTransaction;
import edu.brown.hstore.txns.LocalTransaction;
import edu.brown.interfaces.Configurable;
import edu.brown.interfaces.DebugContext;
import edu.brown.interfaces.Shutdownable;
import edu.brown.logging.LoggerUtil;
import edu.brown.logging.LoggerUtil.LoggerBoolean;
import edu.brown.profilers.TransactionQueueManagerProfiler;
import edu.brown.utils.EventObservable;
import edu.brown.utils.EventObserver;
import edu.brown.utils.ExceptionHandlingRunnable;
import edu.brown.utils.PartitionSet;
import edu.brown.utils.StringUtil;

/**
 * 
 * @author pavlo
 */
public class TransactionQueueManager extends ExceptionHandlingRunnable implements Shutdownable, Configurable {
    private static final Logger LOG = Logger.getLogger(TransactionQueueManager.class);
    private static final LoggerBoolean debug = new LoggerBoolean();
    private static final LoggerBoolean trace = new LoggerBoolean();
    static {
        LoggerUtil.attachObserver(LOG, debug, trace);
    }
    
    // ----------------------------------------------------------------------------
    // STATIC CONFIGURATION
    // ----------------------------------------------------------------------------
    
    private static final int THREAD_WAIT_TIME = 1; // 0.5 millisecond
    private static final TimeUnit THREAD_WAIT_TIMEUNIT = TimeUnit.MILLISECONDS;
    
    private static final int CHECK_INIT_QUEUE_LIMIT = 10000;
    private static final int CHECK_RESTART_QUEUE_LIMIT = 100;
    
    // ----------------------------------------------------------------------------
    // DATA MEMBERS
    // ----------------------------------------------------------------------------
    
    private final HStoreSite hstore_site;
    private final HStoreConf hstore_conf;
    private final PartitionSet localPartitions;
    private boolean stop = false;
    
    /**
     * PartitionLock Configuration
     */
    private int initThrottleThreshold;
    private double initThrottleRelease;
    
    // ----------------------------------------------------------------------------
    // TRANSACTION PARTITION LOCKS QUEUES
    // ----------------------------------------------------------------------------
    
    /**
     * Contains one queue for every partition managed by this coordinator
     */
    private final PartitionLockQueue[] lockQueues;
    
    private final ReentrantLock lockQueueBarriers[];
    
    /**
     * The last txns that was executed for each partition
     * Our local partitions must be accurate, but we can be off for the remote ones.
     * This should be just the transaction id, since the AbstractTransaction handles
     * could have been cleaned up by the time we need this data.
     */
    private final Long[] lockQueueLastTxns;

    private final TransactionQueueManagerProfiler[] profilers;

    // ----------------------------------------------------------------------------
    // TRANSACTIONS THAT NEED TO ADDED TO LOCK QUEUES
    // ----------------------------------------------------------------------------

    /**
     * A queue of transactions that need to be added to the lock queues at the partitions 
     * at this site.
     */
    private final BlockingQueue<AbstractTransaction> initQueue; 

    
    // ----------------------------------------------------------------------------
    // TRANSACTIONS THAT NEED TO BE REQUEUED
    // ----------------------------------------------------------------------------

    /**
     * A queue of aborted transactions that need to restart and add back into the system
     * <B>NOTE:</B> Anything that shows up in this queue will be deleted by this manager
     */
    private final BlockingQueue<Pair<LocalTransaction, Status>> restartQueue;
            // new ConcurrentLinkedQueue<Pair<LocalTransaction, Status>>(); 
    
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
        
        CatalogContext catalogContext = hstore_site.getCatalogContext();
        this.localPartitions = hstore_site.getLocalPartitionIds();
        this.lockQueues = new PartitionLockQueue[catalogContext.numberOfPartitions];
        this.lockQueueLastTxns = new Long[catalogContext.numberOfPartitions];
        this.lockQueueBarriers = new ReentrantLock[catalogContext.numberOfPartitions];
        this.initQueue = new LinkedBlockingQueue<AbstractTransaction>();
        this.restartQueue = new LinkedBlockingQueue<Pair<LocalTransaction,Status>>();
        this.profilers = new TransactionQueueManagerProfiler[catalogContext.numberOfPartitions];
        
        // Initialize internal queues
        for (int partition : this.localPartitions.values()) {
            PartitionLockQueue queue = new PartitionLockQueue(partition,
                                                              hstore_conf.site.txn_incoming_delay,
                                                              this.initThrottleThreshold,
                                                              this.initThrottleRelease);
            this.lockQueues[partition] = queue;
            this.lockQueueBarriers[partition] = new ReentrantLock(true);
            this.profilers[partition] = new TransactionQueueManagerProfiler();
        } // FOR
        Arrays.fill(this.lockQueueLastTxns, Long.valueOf(-1l));
        
        // Use updateConf() to initialize our internal values from the HStoreConf
        this.updateConf(this.hstore_conf, null);
        
        // Add a EventObservable that will tell us when the first non-sysproc
        // request arrives from a client. This will then tell the queues that its ok
        // to increase their limits if they're empty
        hstore_site.getStartWorkloadObservable().addObserver(new EventObserver<HStoreSite>() {
            public void update(EventObservable<HStoreSite> o, HStoreSite arg) {
                for (PartitionLockQueue queue : lockQueues) {
                    if (queue != null) queue.setAllowIncrease(true);
                } // FOR
            };
        });
        
        if (debug.val)
            LOG.debug(String.format("Created %d %s for %s",
                      this.localPartitions.size(), PartitionLockQueue.class.getSimpleName(),
                      hstore_site.getSiteName()));
    }
    
    @Override
    public void updateConf(HStoreConf hstore_conf, String[] changed) {
        this.initThrottleThreshold = (int)(hstore_conf.site.network_incoming_limit_txns * hstore_conf.site.queue_threshold_factor);
        this.initThrottleRelease = hstore_conf.site.queue_release_factor;
        for (PartitionLockQueue queue : this.lockQueues) {
            if (queue != null) {
                queue.setThrottleThreshold(this.initThrottleThreshold);
                queue.setThrottleReleaseFactor(this.initThrottleRelease);
                queue.setAllowDecrease(hstore_conf.site.queue_allow_decrease);
                queue.setAllowIncrease(hstore_conf.site.queue_allow_increase);
                queue.setThrottleThresholdMinSize((int)(this.initThrottleThreshold * hstore_conf.site.queue_min_factor));
                queue.setThrottleThresholdMaxSize((int)(this.initThrottleThreshold * hstore_conf.site.queue_max_factor));
                queue.setThrottleThresholdAutoDelta(hstore_conf.site.queue_autoscale_delta);
                queue.enableProfiling(hstore_conf.site.queue_profiling);
                queue.reset();
            }
        } // FOR
        
    }
    
    // ----------------------------------------------------------------------------
    // RUN METHOD
    // ----------------------------------------------------------------------------
    
    private class Initializer extends ExceptionHandlingRunnable {
        public void runImpl() {
            Thread self = Thread.currentThread();
            self.setName(HStoreThreadManager.getThreadName(hstore_site,
                         HStoreConstants.THREAD_NAME_QUEUE_INIT));
            hstore_site.getThreadManager().registerProcessingThread();
            
            if (debug.val)
                LOG.info(String.format("Starting %s thread", this.getClass().getSimpleName()));
            AbstractTransaction nextTxn = null;
            while (stop == false) {
                try {
                    nextTxn = initQueue.take();
                } catch (InterruptedException ex) {
                    // IGNORE
                }
                if (nextTxn != null) initTransaction(nextTxn);
            } // WHILE
        };
    }
    
    private class Restarter extends ExceptionHandlingRunnable {
        @Override
        public void runImpl() {
            Thread self = Thread.currentThread();
            self.setName(HStoreThreadManager.getThreadName(hstore_site,
                         HStoreConstants.THREAD_NAME_QUEUE_RESTART));
            hstore_site.getThreadManager().registerProcessingThread();
            
            if (debug.val)
                LOG.debug(String.format("Starting %s thread", this.getClass().getSimpleName()));
            Pair<LocalTransaction, Status> pair = null;
            while (stop == false) {
                try {
                    pair = restartQueue.take();
                } catch (InterruptedException ex) {
                    // IGNORE
                }
                LocalTransaction ts = pair.getFirst();
                Status status = pair.getSecond();
                    
                if (trace.val)
                    LOG.trace(String.format("%s - Ready to restart transaction [status=%s]", ts, status));
                Status ret = hstore_site.transactionRestart(ts, status);
                if (trace.val)
                    LOG.trace(String.format("%s - Got return result %s after restarting", ts, ret));
                
                ts.unmarkNeedsRestart();
                hstore_site.queueDeleteTransaction(ts.getTransactionId(), status);
            } // WHILE
        }
    }
    
    /**
     * Every time this thread gets woken up, it locks the queues, loops through the txn_queues,
     * and looks at the lowest id in each queue. If any id is lower than the last_txn id for
     * that partition, it gets rejected and sent back to the caller.
     * Otherwise, the lowest txn_id is popped off and sent to the corresponding partition.
     * Then the thread unlocks the queues and goes back to sleep.
     * If all the partitions are now busy, the thread will wake up when one of them is finished.
     * Otherwise, it will wake up when something else gets added to a queue.
     */
    @Override
    public void runImpl() {
        int numInitialzers = 1;
        int numRestarters = 1;
        
        List<Thread> threads = new ArrayList<Thread>();
        for (int i = 0; i < numInitialzers; i++) {
            Thread t = new Thread(new Initializer());
            t.setDaemon(true);
            t.setUncaughtExceptionHandler(hstore_site.getExceptionHandler());
            t.start();
            threads.add(t);
        } // FOR
        for (int i = 0; i < numRestarters; i++) {
            Thread t = new Thread(new Restarter());
            t.setDaemon(true);
            t.setUncaughtExceptionHandler(hstore_site.getExceptionHandler());
            t.start();
            threads.add(t);
        } // FOR
        
        for (Thread t : threads) {
            try {
                t.join();
            } catch (InterruptedException ex) {
                // IGNORE
                break;
            }
        } // FOR
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
                                       this.lockQueueLastTxns[partition]);
            } // WHILE
        } // SYNCH
        
        // INIT QUEUE
//        while ((ts = this.initQueues[partition].poll()) != null) {
//            TransactionInitQueueCallback callback = ts.getTransactionInitQueueCallback();
//            callback.abort(Status.ABORT_REJECT);
//        } // WHILE
        
        // RESTART QUEUE
        Pair<LocalTransaction, Status> pair = null;
        while ((pair = this.restartQueue.poll()) != null) {
            hstore_site.transactionReject(pair.getFirst(), Status.ABORT_REJECT);
        } // WHILE
    }
    
    // ----------------------------------------------------------------------------
    // INIT QUEUES
    // ----------------------------------------------------------------------------

    private boolean initTransaction(AbstractTransaction nextTxn) {
        if (hstore_conf.site.txn_profiling && nextTxn instanceof LocalTransaction) {
            LocalTransaction localTxn = (LocalTransaction)nextTxn;
            if (localTxn.profiler != null) localTxn.profiler.startQueueLock();
        }
        
        PartitionCountingCallback<AbstractTransaction> callback = nextTxn.getInitCallback();
        assert(callback.isInitialized()) :
            String.format("Unexpected uninitialized %s for %s\n%s",
                          callback.getClass().getSimpleName(),
                          nextTxn, callback.toString());
        boolean ret = (callback.isAborted() == false);
        Status status = null;
        
        if (trace.val)
            LOG.trace(String.format("Adding %s to lock queus for partitions %s\n%s",
                      nextTxn, nextTxn.getPredictTouchedPartitions(), callback));
        for (int partition : nextTxn.getPredictTouchedPartitions().values()) {
            // Skip any non-local partition
            if (this.lockQueues[partition] == null) continue;
            
            // If this txn gets rejected when we try to insert it, then we 
            // just need to stop trying to add it to other partitions
            if (ret) {
                status = this.lockQueueInsert(nextTxn, partition, callback);
                if (status != Status.OK) ret = false;
            // IMPORTANT: But we still need to go through and decrement the
            // callback's counter for those other partitions.
            } else {
                callback.decrementCounter(partition);
            }
        } // FOR
        if (trace.val && ret) {
            LOG.trace(String.format("Finished processing lock queues for %s [result=%s]",
                      nextTxn, ret));
        }
        return (ret);
    }
    
    /**
     * Queue a brand new transaction at this HStoreSite to be added into
     * the appropriate lock queues for the partitions that it needs to access.
     * @param ts
     */
    protected void queueTransactionInit(AbstractTransaction ts) {
        if (debug.val)
            LOG.debug(String.format("Adding %s to initialization queue", ts));
        if (hstore_conf.site.txn_profiling && ts instanceof LocalTransaction) {
            LocalTransaction localTxn = (LocalTransaction)ts;
            if (localTxn.profiler != null) localTxn.profiler.startInitQueue();
        }
        this.initQueue.add(ts);
 }
    
    /**
     * Add a new transaction to this queue manager.
     * Returns true if the transaction was successfully inserted at all partitions.
     * <B>Note:</B> This should not be called directly. You probably want to use initTransaction().
     * @param ts
     * @param partitions
     * @param callback
     * @return
     */
    protected Status lockQueueInsert(AbstractTransaction ts,
                                     int partition,
                                     PartitionCountingCallback<? extends AbstractTransaction> callback) {
        if (hstore_conf.site.queue_profiling) profilers[partition].init_time.start();
        assert(ts.isInitialized()) :
            String.format("Unexpected uninitialized transaction %s [partition=%d]", ts, partition);
        assert(this.hstore_site.isLocalPartition(partition)) :
            String.format("Trying to add %s to non-local partition %d", ts, partition);

        // This is actually bad and should never happen. But for the sake of trying
        // to get the experiments working, we're just going to ignore it...
        if (callback.isInitialized() == false) {
            LOG.warn(String.format("Unexpected uninitialized %s for %s [partition=%d]",
                     callback.getClass().getSimpleName(), ts, partition));
            if (hstore_conf.site.queue_profiling) profilers[partition].init_time.stopIfStarted();
            return (Status.ABORT_UNEXPECTED);
        }
        
        if (debug.val)
            LOG.debug(String.format("Adding %s into lockQueue for partition %d [allPartitions=%s]",
                      ts, partition, ts.getPredictTouchedPartitions()));
        
        // We can preemptively check whether this txnId is greater than
        // the largest one that we know about at a partition
        // We don't need to acquire the lock on last_txns at this partition because 
        // all that we care about is that whatever value is in there now is greater than
        // the what the transaction was trying to use.
        // 2012-12-03 - There is a race condition here where we may get back the last txn that 
        // was released but then it was deleted and cleaned-up. This means that its txn id
        // might be null. A better way to do this is to only have each PartitionExecutor
        // insert the new transaction into its queue. 
        Long txn_id = ts.getTransactionId();
        Long next_safe_id = null;
        Status status = Status.OK;
        
        this.lockQueueBarriers[partition].lock();
        try {
            next_safe_id = this.lockQueues[partition].noteTransactionRecievedAndReturnLastSafeTxnId(txn_id);
        } finally {
            this.lockQueueBarriers[partition].unlock();
        } // SYNCH
        
        // The next txnId that we're going to try to execute is already greater
        // than this new txnId that we were given! Rejection!
        if (next_safe_id != null && next_safe_id.compareTo(txn_id) > 0) {
             if (debug.val)
                LOG.warn(String.format("The next safe lockQueue txn for partition #%d is %s but this " +
                         "is greater than our new txn %s. Rejecting...",
                         partition, next_safe_id, ts));
             status = Status.ABORT_RESTART;
        }
        // Our queue is overloaded. We have to reject the txnId!
        else {
            boolean ret = false;
            if (ts.isPredictSinglePartition() || callback.isAborted() == false) {
                // 2013-04-04
                // I think that we don't need to hold the lockQueueBarrier for this part here,
                // because the PartitionLockQueue will already have an internal lock...
                ret = this.lockQueues[partition].offer(ts, ts.isSysProc());
            }
            if (ret == false) {
                if (debug.val)
                    LOG.debug(String.format("The initQueue for partition #%d is overloaded. " +
                              "Throttling %s until id is greater than %s [queueSize=%d]",
                              partition, ts, next_safe_id, this.lockQueues[partition].size()));
                status = Status.ABORT_REJECT;
            }
        }
        

        // Reject the txn
        if (status != Status.OK) {
            if (hstore_conf.site.queue_profiling) profilers[partition].rejection_time.start();
            this.rejectTransaction(ts, status, partition, next_safe_id);
            if (hstore_conf.site.queue_profiling) {
                profilers[partition].rejection_time.stopIfStarted();
                profilers[partition].init_time.stopIfStarted();
            }
        }
        else if (trace.val) {
            LOG.trace(String.format("Added %s to initQueue for partition %d [queueSize=%d]",
                      ts, partition, this.lockQueues[partition].size()));
        }
        if (hstore_conf.site.queue_profiling) profilers[partition].init_time.stopIfStarted();
        return (status);
    }
    
    /**
     * Check whether there are any transactions that need to be released for execution
     * at the partitions controlled by this queue manager
     * Returns true if we released a transaction at at least one partition
     */
    protected AbstractTransaction checkLockQueue(int partition) throws InterruptedException {
        if (hstore_conf.site.queue_profiling) profilers[partition].lock_time.start();
        if (trace.val)
            LOG.trace(String.format("Checking lock queue for partition %d [queueSize=%d]",
                      partition, this.lockQueues[partition].size()));
        
        // Poll the queue and get the next value.
        AbstractTransaction nextTxn = null;
        this.lockQueueBarriers[partition].lockInterruptibly();
        try {
            nextTxn = this.lockQueues[partition].poll();
        } finally {
            this.lockQueueBarriers[partition].unlock();
        } // SYNCH
        
        if (nextTxn == null) {
            if (hstore_conf.site.queue_profiling) profilers[partition].lock_time.stopIfStarted();
            return (nextTxn);
        }

        PartitionCountingCallback<AbstractTransaction> callback = nextTxn.getInitCallback();
        assert(callback.isInitialized()) :
            String.format("Uninitialized %s callback for %s [hashCode=%d]",
                          callback.getClass().getSimpleName(), nextTxn, callback.hashCode());
        
        // HACK
        if (nextTxn.isAborted()) {
            if (debug.val)
                LOG.warn(String.format("The next txn for partition %d is %s but it is marked as aborted.",
                          partition, nextTxn));
            callback.decrementCounter(partition);
            nextTxn = null;
        }
        // If this callback has already been aborted, then there is nothing we need to
        // do. Somebody else will make sure that this txn is removed from the queue
        else if (callback.isAborted()) {
            if (debug.val)
                LOG.warn(String.format("The next txn for partition %d is %s but its %s is " +
                          "marked as aborted. [queueSize=%d]",
                          partition, nextTxn, callback.getClass().getSimpleName(),
                          this.lockQueues[partition].size()));
            callback.decrementCounter(partition);
            nextTxn = null;
        }
        // We have something we can use
        else {
            if (trace.val)
                LOG.trace(String.format("Good news! Partition %d is ready to execute %s! " +
                          "Invoking %s.run()",
                          partition, nextTxn, callback.getClass().getSimpleName()));
            this.lockQueueLastTxns[partition] = nextTxn.getTransactionId();
        }
        
        
        if (nextTxn != null) {
            // Send the init request for the specified partition
            if (debug.val) 
                LOG.debug(String.format("%s - Invoking %s.run() for partition %d",
                          nextTxn, nextTxn.getInitCallback().getClass().getSimpleName(), partition));
            try {
                nextTxn.getInitCallback().run(partition);
            } catch (NullPointerException ex) {
                // HACK: Ignore...
                if (debug.val)
                    LOG.warn(String.format("Unexpected error when invoking %s for %s at partition %d",
                             nextTxn.getInitCallback().getClass().getSimpleName(),
                             nextTxn, partition), ex);
            } catch (Throwable ex) {
                String msg = String.format("Failed to invoke %s for %s at partition %d",
                                           nextTxn.getInitCallback().getClass().getSimpleName(),
                                           nextTxn, partition);
                throw new ServerFaultException(msg, ex, nextTxn.getTransactionId());
            }
            
            // Mark the txn being released to the given partition
            nextTxn.markReleased(partition);
        
            if (trace.val && nextTxn != null)
                LOG.trace(String.format("Finished processing lock queue for partition %d [next=%s]",
                          partition, nextTxn));
        }
        if (hstore_conf.site.queue_profiling) profilers[partition].lock_time.stopIfStarted();
        return (nextTxn);
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
            String.format("Unexpected uninitialized transaction %s [status=%s, partition=%d]",
                          ts, status, partition);
        assert(ts.getPredictTouchedPartitions().contains(partition)) :
            String.format("Trying to remove %s from partition %d lock queue but it " +
            		     "is not one of its original partitions: %s",
                          ts, partition, ts.getPredictTouchedPartitions());
        assert(this.hstore_site.isLocalPartition(partition)) :
            "Trying to mark txn #" + ts + " as finished on remote partition #" + partition;
        
        // If the given txnId is the current transaction at this partition and still holds
        // the lock on the partition, then we want to make sure that we don't have to
        // look into the queue to see if it's in there.
        // Note that this is always thread-safe because we will release the lock
        // only if we are the current transaction at this partition
        boolean checkQueue = true;
        if (this.lockQueueLastTxns[partition].equals(ts.getTransactionId())) {
            if (trace.val)
                LOG.trace(String.format("%s is the last txn released at partition %d",
                          ts, partition));
            checkQueue = false;
        }
        
        // Always attempt to remove it from this partition's queue
        // If this remove() returns false, then we know that our transaction wasn't
        // sitting in the queue for that partition.
        boolean removed = false;
//        this.lockQueueBarriers[partition].lock();
        try {
            if (checkQueue) {
                // If it wasn't running, then we need to make sure that we remove it from
                // our initialization queue. Unfortunately this means that we need to traverse
                // the queue to find it and remove.
                removed = this.lockQueues[partition].remove(ts);
                if (debug.val && removed)
                    LOG.warn(String.format("Removed %s from partition %d queue", ts, partition));
            }
            
            // Calling contains() is super slow, so we'll only do this if we have tracing enabled
            if (trace.val) {
                assert(this.lockQueues[partition].contains(ts) == false) :
                    String.format("The %s for partition %d contains %s even though it should not! " +
                    		      "[checkQueue=%s, removed=%s]",
                    		      this.lockQueues[partition].getClass().getSimpleName(), partition,
                    		      checkQueue, removed);
            }
            
            // Make sure that if this txn is being aborted, that everyone
            // that is part of it knows what's going on.
            PartitionCountingCallback<AbstractTransaction> callback = ts.getInitCallback();
            callback.decrementCounter(partition);
        } finally {
//            this.lockQueueBarriers[partition].unlock();
        } // SYNCH
        
        if (debug.val)
            LOG.warn(String.format("%s is finished on partition %d " +
                     "[status=%s, checkQueue=%s, removed=%s]",
                     ts, partition, status, checkQueue, removed));
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

        // Always report the txn as aborted so that we can make sure
        // that the callback's counter is decremented properly.
        PartitionCountingCallback<AbstractTransaction> callback = ts.getInitCallback();
        try {
            callback.abort(reject_partition, status);
        } catch (Throwable ex) {
            String msg = String.format("Unexpected error when trying to abort txn %s " +
                                       "[status=%s, rejectPartition=%d, rejectTxnId=%s]\n" +
                                       "Failed Callback: %s",
                                       ts, status, reject_partition, reject_txnId, callback);
            if (debug.val) LOG.warn(msg, ex); 
            throw new RuntimeException(msg, ex);
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
        synchronized (this.lockQueueLastTxns[partition]) {
            if (this.lockQueueLastTxns[partition].compareTo(txn_id) < 0) {
                if (debug.val) LOG.debug(String.format("Marking txn #%d as last txnId for remote partition %d", txn_id, partition));
                this.lockQueueLastTxns[partition] = txn_id;
            }
        } // SYNCH
    }
    
    // ----------------------------------------------------------------------------
    // RESTART QUEUE MANAGEMENT
    // ----------------------------------------------------------------------------

    /**
     * Queue a transaction that was aborted so it can be restarted later on.
     * This is a non-blocking call.
     * The transaction could be queued for deletion before this method returns.
     * @param ts
     * @param status
     */
    public void restartTransaction(LocalTransaction ts, Status status) {
        assert(ts != null) :
            String.format("Unexpected null transaction %s [status=%s]", ts, status);
        assert(ts.isInitialized()) :
            String.format("Unexpected uninitialized transaction %s [status=%s]", ts, status);
        
        if (debug.val)
            LOG.debug(String.format("%s - Requeing transaction for execution [status=%s]", ts, status));
        ts.markNeedsRestart();
        
        if (this.restartQueue.offer(Pair.of(ts, status)) == false) {
            if (debug.val)
                LOG.debug(String.format("%s - Unable to add txn to restart queue. Rejecting...", ts));
            this.hstore_site.transactionReject(ts, Status.ABORT_REJECT);
            ts.unmarkNeedsRestart();
            this.hstore_site.queueDeleteTransaction(ts.getTransactionId(), Status.ABORT_REJECT);
            return;
        }
        if (debug.val)
            LOG.debug(String.format("%s - Successfully added txn to restart queue.", ts));
    }
    
    private void checkRestartQueue() {
        if (debug.val && this.restartQueue.isEmpty() == false)
            LOG.trace(String.format("Checking whether we can restart %d held txns",
                      this.restartQueue.size()));
        
        Pair<LocalTransaction, Status> pair = null;
        int limit = CHECK_RESTART_QUEUE_LIMIT;
        while ((pair = this.restartQueue.poll()) != null) {
            LocalTransaction ts = pair.getFirst();
            Status status = pair.getSecond();
            
            if (trace.val)
                LOG.trace(String.format("%s - Ready to restart transaction [status=%s]", ts, status));
            Status ret = this.hstore_site.transactionRestart(ts, status);
            if (trace.val)
                LOG.trace(String.format("%s - Got return result %s after restarting", ts, ret));
            
            ts.unmarkNeedsRestart();
            this.hstore_site.queueDeleteTransaction(ts.getTransactionId(), status);
            if (limit-- == 0) break;
        } // WHILE
    }
    
    // ----------------------------------------------------------------------------
    // UTILITY METHODS
    // ----------------------------------------------------------------------------
    
    public PartitionLockQueue getLockQueue(int partition) {
        return (this.lockQueues[partition]);
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
        Map<String, Object> m[] = (Map<String, Object>[])new Map[1];
        int idx = -1;

        // Local Partitions
        m[++idx] = new LinkedHashMap<String, Object>();
        for (int partition = 0; partition < this.lockQueueLastTxns.length; partition++) {
            Map<String, Object> inner = new LinkedHashMap<String, Object>();
            inner.put("Current Txn", this.lockQueueLastTxns[partition]);
            if (this.localPartitions.contains(partition)) {
                inner.put("Queue Size", this.lockQueues[partition].size());
            }
            m[idx].put(String.format("Partition #%02d", partition), inner);
        } // FOR
        
        return StringUtil.formatMaps(m);
    }
    
    public String debug() {
        String debug[] = new String[this.lockQueues.length];
        int idx = 0;
        for (int partition : this.localPartitions.values()) {
            debug[idx++] = this.lockQueues[partition].debug();
        } // FOR
        int max_width = StringUtil.maxWidth(debug);
        String line = StringUtil.repeat("-", max_width) + "\nCONTENTS:\n";
        
        idx = 0;
        for (int partition : this.localPartitions.values()) {
            debug[idx++] += line + StringUtil.join("\n", this.lockQueues[partition]); 
        } // FOR
        return (StringUtil.columns(debug));
    }
    
    // ----------------------------------------------------------------------------
    // DEBUG METHODS
    // ----------------------------------------------------------------------------

    public class Debug implements DebugContext {
        public int getInitQueueSize() {
            return (initQueue.size());
        }
        public int getLockQueueSize() {
            Set<AbstractTransaction> allTxns = new HashSet<AbstractTransaction>();
            for (int p : localPartitions.values()) {
                try {
                    allTxns.addAll(lockQueues[p]);
                } catch (ConcurrentModificationException ex) {
                    // IGNORE
                }
            }
            return (allTxns.size());
        }
        public int getLockQueueSize(int partition) {
            return (lockQueues[partition].size());
        }
        public int getRestartQueueSize() {
            return (restartQueue.size());
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
         * Return the current transaction that is executing at this partition
         * <b>NOTE:</b> This is not thread-safe.
         * @param partition
         * @return
         */
        public Long getCurrentTransaction(int partition) {
            return (lockQueueLastTxns[partition]);
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
