package edu.brown.hstore;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.log4j.Logger;
import org.voltdb.TransactionIdManager;

import edu.brown.hstore.conf.HStoreConf;
import edu.brown.hstore.txns.AbstractTransaction;
import edu.brown.hstore.util.ThrottlingQueue;
import edu.brown.interfaces.DebugContext;
import edu.brown.logging.LoggerUtil;
import edu.brown.logging.LoggerUtil.LoggerBoolean;
import edu.brown.profilers.PartitionLockQueueProfiler;
import edu.brown.utils.StringUtil;

/**
 * <p>Extends a PriorityQueue such that is only stores transaction state
 * objects, and it only releases them (to a poll() call) if they are
 * ready to be processed.</p>
 *
 * <p>In this case, ready to be processed is determined by storing the
 * most recent transaction id from each initiator. The smallest transaction
 * id across all initiators is safe to run. Also any older transactions are
 * also safe to run.</p>
 *
 * <p>This class manages all that state.</p>
 * 
 */
public class PartitionLockQueue extends ThrottlingQueue<AbstractTransaction> {
    protected static final Logger LOG = Logger.getLogger(PartitionLockQueue.class);
    private static final LoggerBoolean debug = new LoggerBoolean();
    private static final LoggerBoolean trace = new LoggerBoolean();
    static {
        LoggerUtil.attachObserver(LOG, debug, trace);
    }
    
    // ----------------------------------------------------------------------------
    // STATIC MEMBERS
    // ----------------------------------------------------------------------------
    
    public enum QueueState {
        UNBLOCKED,
        BLOCKED_EMPTY,
        BLOCKED_ORDERING,
        BLOCKED_SAFETY;
    }
    
    /**
     * Special marker to indicate that we have no set a blocking timestamp
     * for the next txn to release.
     */
    private static final long NULL_BLOCK_TIMESTAMP = -1l;
    
    // ----------------------------------------------------------------------------
    // INTERNAL STATE
    // ----------------------------------------------------------------------------

    private final int partitionId;
    private int maxWaitTime;
    
    private final ReentrantLock lock = new ReentrantLock(true);
    private final Condition isReady = lock.newCondition();
    
    /**
     * This is the timestamp (in milliseconds) when we can unblock
     * the next transaction in the queue.
     * <B>Note:</B> Do not manipulate this outside of a synchronized block.
     */
    private long blockTimestamp = NULL_BLOCK_TIMESTAMP;

    /**
     * The current state of the queue
     * <B>Note:</B> Do not manipulate this outside of a synchronized block.
     */
    private QueueState state = QueueState.BLOCKED_EMPTY;
    
    private long txnsPopped = 0;
    private Long lastSeenTxnId = -1l;
    private Long lastSafeTxnId = -1l;
    private Long lastTxnPopped = -1l;
    
    private final PartitionLockQueueProfiler profiler;
    
    // ----------------------------------------------------------------------------
    // INITIALIZATION
    // ----------------------------------------------------------------------------
    
    /**
     * Constructor
     * @param partitionId
     * @param maxWaitTime
     * @param throttle_threshold TODO
     * @param throttle_release TODO
     * @param hstore_site
     */
    public PartitionLockQueue(int partitionId, int maxWaitTime, int throttle_threshold, double throttle_release) {
        super(new PriorityBlockingQueue<AbstractTransaction>(), throttle_threshold, throttle_release);
        
        this.partitionId = partitionId;
        this.maxWaitTime = maxWaitTime;
        
        if (HStoreConf.singleton().site.queue_profiling) {
            this.profiler = new PartitionLockQueueProfiler();
        } else {
            this.profiler = null;
        }
    }
    
    // ----------------------------------------------------------------------------
    // UTILITY METHODS
    // ----------------------------------------------------------------------------
    
    /**
     * Get the current state of the queue.
     * <B>Note:</B> This is not thread safe.
     * @return
     */
    protected QueueState getQueueState() {
        return (this.state);
    }
    
    protected int getPartitionId() {
        return (this.partitionId);
    }
    
    public Long getLastTransactionId() {
        return (this.lastTxnPopped);
    }
    
    // ----------------------------------------------------------------------------
    // POLL/TAKE METHODS
    // ----------------------------------------------------------------------------
    
    /**
     * Only return transaction state objects that are ready to run.
     * This is non-blocking. If the txn is not ready, then this will return null.
     * <B>Note:</B> This should only be allowed to be called by one thread.
     */
    @Override
    public AbstractTransaction poll() {
        AbstractTransaction retval = null;
        
        if (trace.val)
            LOG.trace(String.format("Partition %d :: Attempting to acquire lock", this.partitionId));
        this.lock.lock();
        try {
            if (this.state == QueueState.BLOCKED_SAFETY || this.state == QueueState.BLOCKED_ORDERING) {
                this.checkQueueState(false);
            }
            if (this.state == QueueState.UNBLOCKED) {

                if (this.state == QueueState.UNBLOCKED) {
                    // 2012-12-21
                    // So this is allow to be null because there is a race condition 
                    // if another thread removes the txn from the queue.
                    retval = super.poll();
                    
                    if (retval != null) {
                        if (debug.val)
                            LOG.debug(String.format("Partition %d :: poll() -> %s",
                                      this.partitionId, retval));
                        this.lastTxnPopped = retval.getTransactionId();
                        this.txnsPopped++;
                    }
                    // call this again to prime the next txn
                    this.checkQueueState(true);
                }
            }
        } finally {
            if (trace.val)
                LOG.trace(String.format("Partition %d :: Releasing lock", this.partitionId));
            this.lock.unlock();
        } // SYNCH
        return (retval);
    }
    
    /**
     * Only return transaction state objects that are ready to run.
     * This method will wait until the transaction's block time has passed.
     * @return
     * @throws InterruptedException
     */
    public AbstractTransaction take() throws InterruptedException {
        AbstractTransaction retval = null;
        
        // Ok now here is the tricky part. We don't have a txn that is 
        // ready to run, so we need to block ourselves until we get one.
        // This could be for two reasons:
        //  (1) The queue is empty.
        //  (2) The waiting period for the next txn hasn't passed yet.
        // 
        // Note that we can't simply attach ourselves to our inner queue because
        // we don't want to get back the txn right when it gets added.
        // We want to wait until the time period has passed.
        if (trace.val)
            LOG.trace(String.format("Partition %d :: Attempting to acquire lock", this.partitionId));
        this.lock.lockInterruptibly();
        try {
            if (debug.val && this.state != QueueState.UNBLOCKED)
                LOG.debug(String.format("Partition %d :: take() -> " +
                          "Current state is %s. Blocking until ready", this.partitionId, this.state));
            while (this.state != QueueState.UNBLOCKED) {
                if (trace.val)
                    LOG.trace(String.format("Partition %d :: take() -> Calculating how long to block",
                              this.partitionId));
                
                long waitTime = -1;
                boolean isEmpty = (this.state == QueueState.BLOCKED_EMPTY);
                boolean needsUpdateQueue = false;
                
                // If the queue isn't empty, then we need to figure out
                // how long we should sleep for
                if (isEmpty == false) {
                    // If we're blocked because of an ordering issue (i.e., we have a new txn
                    // in the system that is less than our current head of the queue, but we 
                    // haven't inserted it yet), then we will want to wait for the full timeout
                    // period. We won't actually have to wait this long because somebody will poke
                    // us after the new txn is added to the queue.
                    if (this.state == QueueState.BLOCKED_ORDERING) {
                        waitTime = this.maxWaitTime;
                    } else { 
                        waitTime = this.blockTimestamp - System.currentTimeMillis();
                    }
                }
                
                try {
                    // If we're empty, then we need to block indefinitely until we're poked
                    if (isEmpty) {
                        if (debug.val)
                            LOG.debug(String.format("Partition %d :: take() -> " +
                            		  "Blocking because queue is empty", this.partitionId));
                        this.isReady.await();
                    }
                    // Otherwise, we'll sleep until our time out and then 
                    // check the queue status for ourselves
                    else if (waitTime > 0) {
                        // We are going to wait for the specified time
                        // The Condition will return true if somebody poked us, which
                        // means that somebody changed the queue state to UNBLOCKED 
                        // for us. If we are woken because of a timeout, then the
                        // return status will be false, which means that we need to
                        // queue state ourself.
                        if (debug.val)
                            LOG.debug(String.format("Partition %d :: take() -> " +
                                      "Blocking for %d ms", this.partitionId, waitTime));
                        needsUpdateQueue = (this.isReady.await(waitTime, TimeUnit.MILLISECONDS) == false);
                    }
                    // Our txn is ready to run now, so we don't need to block
                    else {
                        if (debug.val)
                            LOG.debug(String.format("Partition %d :: take() -> " +
                                      "Ready to retrieve next txn immediately [waitTime=%d, isEmpty=%s]",
                                      this.partitionId, waitTime, isEmpty));
                        needsUpdateQueue = true;
                    }
                } catch (InterruptedException ex) {
                    this.isReady.signal();
                    throw ex;
                }
                
                if (needsUpdateQueue) this.checkQueueState(false);
                
            } // WHILE
            // The next txn is ready to run now!
            assert(this.state == QueueState.UNBLOCKED);
            retval = super.poll();
            
            // 2012-01-06
            // This could be null because there is a race condition if all of the
            // txns are removed by another thread right before we try to
            // poll our queue.
            if (retval != null) {
                this.lastTxnPopped = retval.getTransactionId();
                this.txnsPopped++;
                
                // Call this again to prime the next txn
                this.checkQueueState(true);
            }
            
            if (trace.val)
                LOG.trace(String.format("Partition %d :: take() -> Leaving blocking section",
                          this.partitionId));
        } finally {
            if (trace.val)
                LOG.trace(String.format("Partition %d :: Releasing lock", this.partitionId));
            this.lock.unlock();
        }
        if (debug.val)
            LOG.debug(String.format("Partition %d :: take() -> %s",
                      this.partitionId, retval));
        
        return (retval);
    }

    /**
     * Only return transaction state objects that are ready to run.
     * It is safe to call this from any thread if you need to (but you probably don't)
     */
    @Override
    public AbstractTransaction peek() {
        AbstractTransaction retval = null;
        if (this.state == QueueState.UNBLOCKED) {
            // assert(checkQueueState(false) == QueueState.UNBLOCKED);
            retval = super.peek();
            assert(retval != null);
        }
        if (debug.val)
            LOG.debug(String.format("Partition %d :: peek() -> %s", this.partitionId, retval));
        return (retval);
    }
    
    // ----------------------------------------------------------------------------
    // OFFER METHODS
    // ----------------------------------------------------------------------------
    
    /**
     * Add in a transaction to the queue.
     * It is safe to call this from any thread if you need to
     */
    @Override
    public boolean offer(AbstractTransaction ts, boolean force) {
        assert(ts != null);
        assert(ts.isInitialized()) :
            String.format("Unexpected uninitialized transaction %s [partition=%d]", ts, this.partitionId);
        
        boolean retval = super.offer(ts, force);
        if (debug.val)
            LOG.debug(String.format("Partition %d :: offer(%s) -> %s", this.partitionId, ts, retval));

        if (retval) {
            if (trace.val)
                LOG.trace(String.format("Partition %d :: Attempting to acquire lock", this.partitionId));
            this.lock.lock();
            try {
                if (retval) this.checkQueueState(false);
            } finally {
                if (trace.val)
                    LOG.trace(String.format("Partition %d :: Releasing lock", this.partitionId));
                this.lock.unlock();
            }
        }
        return (retval);
    }
    
    @Override
    @Deprecated
    public boolean offer(AbstractTransaction e) {
        return this.offer(e, false);
    }

    // ----------------------------------------------------------------------------
    // REMOVE METHODS
    // ----------------------------------------------------------------------------
    
    @Override
    public boolean remove(Object obj) {
        AbstractTransaction txn = (AbstractTransaction)obj;
        boolean retval;
        
        if (trace.val)
            LOG.trace(String.format("Partition %d :: Attempting to acquire lock", this.partitionId));
        this.lock.lock();
        try {
            // We have to check whether we are the first txn in the queue,
            // because we will need to reset the blockTimestamp after 
            // delete ourselves so that the next guy can get executed
            // This is not thread-safe...
            boolean reset = txn.equals(super.peek());
            retval = super.remove(txn);
            if (debug.val) {
                LOG.debug(String.format("Partition %d :: remove(%s) -> %s", this.partitionId, txn, retval));
                // Sanity Check
                assert(super.contains(txn) == false) : 
                    "Failed to remove " + txn + "???\n" + this.debug();
            }
            if (retval) this.checkQueueState(reset);
        } finally {
            if (trace.val)
                LOG.trace(String.format("Partition %d :: Releasing lock", this.partitionId));
            this.lock.unlock();
        }
        return (retval);
    }
    
    /**
     * Update the information stored about the latest transaction
     * seen from each initiator. Compute the newest safe transaction id.
     */
    public Long noteTransactionRecievedAndReturnLastSafeTxnId(Long txnId) {
        assert(txnId != null);
        if (debug.val)
            LOG.debug(String.format("Partition %d :: noteTransactionRecievedAndReturnLastSeen(%d)",
                      this.partitionId, txnId));

        this.lastSeenTxnId = txnId;
        if (trace.val) {
            LOG.trace(String.format("Partition %d :: SET lastSeenTxnId = %d",
                      this.partitionId, this.lastSeenTxnId));
            LOG.trace(String.format("Partition %d :: Attempting to acquire lock", this.partitionId));
        }
        this.lock.lock();
        try {
            if (this.lastTxnPopped.compareTo(txnId) > 0) {
                if (debug.val)
                    LOG.warn(String.format("Partition %d :: Txn ordering deadlock --> LastTxn:%d / NewTxn:%d",
                             this.partitionId, this.lastTxnPopped, txnId));
                return (this.lastTxnPopped);
            }
            
            // We always need to check whether this new txnId is less than our next safe txnID
            // If it is, then we know that we need to replace it.
            if (txnId.compareTo(this.lastSafeTxnId) < 0) {
                // 2013-01-15
                // Instead of calling checkQueueState() here, we'll 
                // just change the state real quickly. This should be ok because
                // then we'll immediately insert this new txn into the queue
                // and then update the queue state then.
                this.state = QueueState.BLOCKED_ORDERING;
                this.lastSafeTxnId = txnId;
                if (trace.val)
                        LOG.trace(String.format("Partition %d :: SET lastSafeTxnId = %d",
                                  this.partitionId, this.lastSafeTxnId));

                // Since we know that we just replaced the last safeTxnId, we 
                // need to check our queue state to update ourselves
                // this.checkQueueState(false);
            }
        } finally {
            if (trace.val)
                LOG.trace(String.format("Partition %d :: Releasing lock", this.partitionId));
            this.lock.unlock();
        } // SYNCH
        return (this.lastSafeTxnId);
    }


    // ----------------------------------------------------------------------------
    // INTERNAL STATE CALCULATION
    // ----------------------------------------------------------------------------
    
    /**
     * This is the most important method of the queue.
     * This will figure out the next state and how long we must wait until we 
     * can release the next transaction.
     * <B>Note:</B> I believe that this is the only thing that needs to be synchronized
     * @param afterRemoval If this flag is set to true, then it means that who ever is calling this method
     *                     just removed something from the queue. That means that we need to go and check
     *                     whether the lastSafeTxnId should change.
     * @return
     */
    private QueueState checkQueueState(boolean afterRemoval) {
        if (trace.val && super.isEmpty() == false)
            LOG.trace(String.format("Partition %d :: checkQueueState(afterPoll=%s) [current=%s]",
                      this.partitionId, afterRemoval, this.state));
        QueueState newState = (afterRemoval ? QueueState.BLOCKED_SAFETY : QueueState.UNBLOCKED);
        long currentTimestamp = -1l;
        AbstractTransaction ts = super.peek(); // BLOCKING
        Long txnId = null;
        if (ts == null) {
//            if (trace.val)
//                LOG.trace(String.format("Partition %d :: Queue is empty.", this.partitionId));
            newState = QueueState.BLOCKED_EMPTY;
        }
        // Check whether can unblock now
        else {
            assert(ts.isInitialized()) :
                String.format("Unexpected uninitialized transaction %s [partition=%d]", ts, this.partitionId);
            txnId = ts.getTransactionId();
            // HACK: Ignore null txnIds
            if (txnId == null) {
                LOG.warn(String.format("Partition %d :: Uninitialized transaction handle %s", this.partitionId, ts));
                return (this.state);
            }
            assert(txnId != null) : "Null transaction id from " + txnId;
            
            // If this txnId is greater than the last safe one that we've seen, then we know
            // that the lastSafeTxnId has been polled. That means that we need to 
            // wait for an appropriate amount of time before we're allow to be executed.
            if (txnId.compareTo(this.lastSafeTxnId) > 0 && afterRemoval == false) {
                newState = QueueState.BLOCKED_ORDERING;
                if (debug.val)
                    LOG.debug(String.format("Partition %d :: txnId[%d] > lastSafeTxnId[%d]",
                              this.partitionId, txnId, this.lastSafeTxnId));
            }
            // If our current block time is negative, then we know that we're the first txnId
            // that's been in the system. We'll also want to wait a bit before we're
            // allowed to be executed.
            else if (this.blockTimestamp == NULL_BLOCK_TIMESTAMP) {
                newState = QueueState.BLOCKED_SAFETY;
                if (debug.val)
                    LOG.debug(String.format("Partition %d :: txnId[%d] ==> %s (blockTime=%d)",
                              this.partitionId, txnId, newState, this.blockTimestamp));
            }
            // Check whether it's safe to unblock this mofo
            else if ((currentTimestamp = System.currentTimeMillis()) < this.blockTimestamp) {
                newState = QueueState.BLOCKED_SAFETY;
                if (debug.val)
                    LOG.debug(String.format("Partition %d :: txnId[%d] ==> %s (blockTime[%d] - current[%d] = %d)",
                              this.partitionId, txnId, newState,
                              this.blockTimestamp, currentTimestamp,
                              Math.max(0, this.blockTimestamp - currentTimestamp)));
            }
            // We didn't find any reason to block this txn, so it's sail yo for it...
            else if (debug.val) {
                LOG.debug(String.format("Partition %d :: Safe to Execute %d [currentTime=%d]",
                          this.partitionId, txnId, System.currentTimeMillis()));
            }
        }
        
        if (newState != this.state) {
            // note if we get non-empty but blocked
            if ((newState == QueueState.BLOCKED_ORDERING) || (newState == QueueState.BLOCKED_SAFETY)) {
                if (trace.val)
                    LOG.trace(String.format("Partition %d :: NewState=%s --> %s",
                              this.partitionId, newState, ts));
                if (currentTimestamp == -1) currentTimestamp = System.currentTimeMillis();
                long txnTimestamp = TransactionIdManager.getTimestampFromTransactionId(txnId.longValue());
                
                // Calculate how long we need to wait before this txn is safe to run
                // If we're blocking on "safety", then we can use an offset based 
                // on when the txnId was created. If we're blocking for "ordering",
                // then we'll want to wait for the full wait time.
                int waitTime = this.maxWaitTime;
                if (newState == QueueState.BLOCKED_SAFETY) {
                    waitTime = this.maxWaitTime - (int)(currentTimestamp - txnTimestamp);
                    if (waitTime > this.maxWaitTime) {
                        waitTime = this.maxWaitTime;
                    } else if (waitTime < 0) {
                        waitTime = 0;
                    }
                }
                
                this.blockTimestamp = currentTimestamp + waitTime;
                if (debug.val)
                    LOG.debug(String.format("Partition %d :: SET blockTimestamp = %d --> %s [waitTime=%d, txnTimestamp=%d]",
                              this.partitionId, this.blockTimestamp, ts, waitTime, txnTimestamp));
                
                if (this.blockTimestamp <= currentTimestamp) {
                    newState = QueueState.UNBLOCKED;
                }
                if (this.profiler != null && this.lastSafeTxnId.equals(txnId) == false)
                    this.profiler.waitTimes.put(newState == QueueState.UNBLOCKED ? 0 : waitTime);
                
                if (debug.val) {
                    String traceOutput = "";
                    if (trace.val) {
                        Map<String, Object> m = new LinkedHashMap<String, Object>();
                        m.put("Txn Init Timestamp", txnTimestamp);
                        m.put("Current Timestamp", currentTimestamp);
                        m.put("Block Time Remaining", (this.blockTimestamp - currentTimestamp));
                        traceOutput = "\n" + StringUtil.formatMaps(m);
                    }
                    LOG.debug(String.format("Partition %d :: Blocking %s for %d ms " +
                    		  "[maxWait=%d, origState=%s, newState=%s]\n%s%s",
                              this.partitionId, ts, (this.blockTimestamp - currentTimestamp),
                              this.maxWaitTime, this.state, newState, this.debug(), traceOutput));
                }
            }
            else if (newState == QueueState.UNBLOCKED) {
                if (currentTimestamp == -1) currentTimestamp = System.currentTimeMillis();
                if (this.blockTimestamp > currentTimestamp) {
                    newState = QueueState.BLOCKED_SAFETY;
                }
            }
        } // IF

        // This txn should always becomes our next safeTxnId.
        // This is essentially the next txn
        // that should be executed, but somebody *could* come along and add in 
        // a new txn with a lower id. But that's ok because we've synchronized setting
        // the id up above. This is actually probably the only part of this entire method
        // that needs to be protected...
        if (txnId != null) this.lastSafeTxnId = txnId;
        
        // Set the new state
        if (newState != this.state) {
            if (trace.val)
                LOG.trace(String.format("Partition %d :: ORIG[%s]->NEW[%s] / LastSafeTxn:%d",
                          this.partitionId, this.state, newState, this.lastSafeTxnId));
            if (this.profiler != null) {
                this.profiler.queueStates.get(this.state).stopIfStarted();
                this.profiler.queueStates.get(newState).start();
            }
            this.state = newState;
            
            // Always poke anybody that is blocking on this queue.
            // The txn may not be ready to run just yet, but at least they'll be
            // able to recompute a new sleep time.
            this.isReady.signal();
        }
        else if (this.profiler != null) {
            this.profiler.queueStates.get(this.state).restart();
        }
            
        // Sanity Check
        if ((this.state == QueueState.BLOCKED_ORDERING) || (this.state == QueueState.BLOCKED_SAFETY)) {
            assert(this.state != QueueState.BLOCKED_EMPTY);
        }
        
        // Make sure that we're always in a valid state to avoid livelock problems
        assert(this.state != QueueState.BLOCKED_SAFETY || 
              (this.state == QueueState.BLOCKED_SAFETY && this.blockTimestamp != NULL_BLOCK_TIMESTAMP)) :
              String.format("Invalid state %s with NULL blocked timestamp", this.state);
        assert(this.state != QueueState.BLOCKED_ORDERING ||
              (this.state == QueueState.BLOCKED_ORDERING && this.blockTimestamp != NULL_BLOCK_TIMESTAMP)) :
              String.format("Invalid state %s with NULL blocked timestamp", this.state);
        return this.state;
    }
    
    // ----------------------------------------------------------------------------
    // DEBUG METHODS
    // ----------------------------------------------------------------------------
    
    public class Debug implements DebugContext {
        public long getTransactionsPopped() {
            return (txnsPopped);
        }
        public long getBlockedTimestamp() {
            return (blockTimestamp);
        }
        public PartitionLockQueueProfiler getProfiler() {
            return (profiler);
        }
        public QueueState checkQueueState() {
            QueueState ret = null;
            lock.lock();
            try {
                ret = PartitionLockQueue.this.checkQueueState(false);
            } finally {
                lock.unlock();
            }
            return (ret);
        }
        protected void setMaxWaitTime(int maxWaitTime) {
            PartitionLockQueue.this.maxWaitTime = maxWaitTime;
        }
    }
    
    private PartitionLockQueue.Debug cachedDebugContext;
    public PartitionLockQueue.Debug getDebugContext() {
        if (cachedDebugContext == null) {
            // We don't care if we're thread-safe here...
            this.cachedDebugContext = new Debug();
        }
        return (cachedDebugContext);
    }
    
    public String debug() {
        long timestamp = System.currentTimeMillis();
        AbstractTransaction peek = super.peek();
        
        @SuppressWarnings("unchecked")
        Map<String, Object> m[] = new Map[3];
        int i = -1;
        
        m[++i] = new LinkedHashMap<String, Object>();
        m[i].put("PartitionId", this.partitionId);
        m[i].put("Current State", this.state);
        m[i].put("# of Elements", this.size());
        m[i].put("# of Popped", this.txnsPopped);
        m[i].put("Last Popped Txn", this.lastTxnPopped);
        m[i].put("Last Seen Txn", this.lastSeenTxnId);
        m[i].put("Last Safe Txn", this.lastSafeTxnId);
        
        m[++i] = new LinkedHashMap<String, Object>();
        m[i].put("Throttled", super.isThrottled());
        m[i].put("Threshold", super.getThrottleThreshold());
        m[i].put("Release", super.getThrottleRelease());
        m[i].put("Increase Delta", super.getThrottleThresholdIncreaseDelta());
        m[i].put("Max Size", super.getThrottleThresholdMaxSize());

        m[++i] = new LinkedHashMap<String, Object>();
        m[i].put("Peek Txn", (peek == null ? "null" : peek));
        m[i].put("Wait Time", this.maxWaitTime + " ms");
        m[i].put("Current Time", timestamp);
        m[i].put("Blocked Time", (this.blockTimestamp > 0 ? this.blockTimestamp + (this.blockTimestamp < timestamp ? " **PASSED**" : "") : "--"));
        m[i].put("Blocked Remaining", (this.blockTimestamp > 0 ? Math.max(0, this.blockTimestamp - timestamp) + " ms" : "--"));
        
        return (StringUtil.formatMaps(m));
    }
}
