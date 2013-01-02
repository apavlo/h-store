package edu.brown.hstore;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.PriorityBlockingQueue;

import org.apache.log4j.Logger;
import org.voltdb.TransactionIdManager;
import org.voltdb.utils.EstTime;

import edu.brown.hstore.conf.HStoreConf;
import edu.brown.hstore.txns.AbstractTransaction;
import edu.brown.interfaces.DebugContext;
import edu.brown.logging.LoggerUtil;
import edu.brown.logging.LoggerUtil.LoggerBoolean;
import edu.brown.profilers.TransactionInitPriorityQueueProfiler;
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
 * <B>NOTE:</B> Do not put any synchronized blocks in this. All synchronization
 * should be done by the caller.
 */
public class TransactionInitPriorityQueue extends PriorityBlockingQueue<AbstractTransaction> {
    private static final long serialVersionUID = 573677483413142310L;
    protected static final Logger LOG = Logger.getLogger(TransactionInitPriorityQueue.class);
    private static final LoggerBoolean debug = new LoggerBoolean(LOG.isDebugEnabled());
    private static final LoggerBoolean trace = new LoggerBoolean(LOG.isTraceEnabled());
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
    private final int waitTime;
    
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
    
    private final TransactionInitPriorityQueueProfiler profiler;
    
    // ----------------------------------------------------------------------------
    // INITIALIZATION
    // ----------------------------------------------------------------------------
    
    /**
     * Constructor
     * @param hstore_site
     * @param partitionId
     * @param wait
     */
    public TransactionInitPriorityQueue(int partitionId, int wait) {
        super();
        this.partitionId = partitionId;
        this.waitTime = wait;
        
        if (HStoreConf.singleton().site.queue_profiling) {
            this.profiler = new TransactionInitPriorityQueueProfiler();
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
    
    // ----------------------------------------------------------------------------
    // QUEUE OPERATION METHODS
    // ----------------------------------------------------------------------------
    
    /**
     * Only return transaction state objects that are ready to run.
     * <B>Note:</B> This should only be allowed to be called by one thread.
     */
    @Override
    public AbstractTransaction poll() {
        AbstractTransaction retval = null;
        if (this.state == QueueState.UNBLOCKED) {
            // 2012-12-21
            // So this is allow to be null because there is a race condition 
            // if another thread removes the txn from the queue.
            retval = super.poll();
            if (retval != null) {
                if (debug.val) LOG.debug(String.format("Partition %d :: poll() -> %s",
                                         this.partitionId, retval));
                this.lastTxnPopped = retval.getTransactionId();
                this.txnsPopped++;
            }
            // call this again to prime the next txn
            this.checkQueueState(true);
        }
        return retval;
    }

    /**
     * Only return transaction state objects that are ready to run.
     * It is safe to call this from any thread if you need to (but you probably don't)
     */
    @Override
    public AbstractTransaction peek() {
        AbstractTransaction retval = null;
        if (this.state == QueueState.UNBLOCKED) {
            assert(checkQueueState(false) == QueueState.UNBLOCKED);
            retval = super.peek();
            assert(retval != null);
        }
        if (debug.val) LOG.debug(String.format("Partition %d :: peek() -> %s",
                                 this.partitionId, retval));
        return retval;
    }
    
    /**
     * Add in a transaction to the queue.
     * It is safe to call this from any thread if you need to
     */
    @Override
    public boolean offer(AbstractTransaction ts) {
        assert(ts != null);
        assert(ts.isInitialized()) :
            String.format("Unexpected uninitialized transaction %s [partition=%d]", ts, this.partitionId);
        
        boolean retval = super.offer(ts);
        if (debug.val) LOG.debug(String.format("Partition %d :: offer(%s) -> %s", this.partitionId, ts, retval));
        if (retval) this.checkQueueState(false);
        return retval;
    }

    @Override
    public boolean remove(Object obj) {
        AbstractTransaction txn = (AbstractTransaction)obj;
        
        // We have to check whether we are the first txn in the queue,
        // because we will need to reset the blockTimestamp after 
        // delete ourselves so that the next guy can get executed
        // This is not thread-safe...
        boolean reset = txn.equals(super.peek());
        boolean retval = super.remove(txn);
        // Sanity Check
        assert(super.contains(txn) == false) :
            "Failed to remove " + txn + "???\n" + this.debug();
        if (debug.val) LOG.warn(String.format("Partition %d :: remove(%s) -> %s",
                                this.partitionId, txn, retval));
        if (retval) this.checkQueueState(reset);
        return retval;
    }
    
    /**
     * Update the information stored about the latest transaction
     * seen from each initiator. Compute the newest safe transaction id.
     */
    public Long noteTransactionRecievedAndReturnLastSeen(Long txnId) {
        assert(txnId != null);
        if (trace.val) LOG.trace(String.format("Partition %d :: noteTransactionRecievedAndReturnLastSeen(%d)",
                                 this.partitionId, txnId));

        // we've decided that this can happen, and it's fine... just ignore it
        if (debug.val) {
            if (this.lastTxnPopped != null && this.lastTxnPopped.compareTo(txnId) > 0) {
                LOG.warn(String.format("Txn ordering deadlock at Partition %d ::> LastTxn: %d / NewTxn: %d",
                                       this.partitionId, this.lastTxnPopped, txnId));
                LOG.warn("LAST: " + this.lastTxnPopped);
                LOG.warn("NEW:  " + txnId);
            }
        }

        this.lastSeenTxnId = txnId;
        if (trace.val) LOG.trace(String.format("Partition %d :: SET lastSeenTxnId = %d",
                                 this.partitionId, this.lastSeenTxnId));
        if (txnId.compareTo(this.lastSafeTxnId) < 0) {
            synchronized (this) {
                if (txnId.compareTo(this.lastSafeTxnId) < 0) {
                    this.lastSafeTxnId = txnId;
                    if (trace.val) LOG.trace(String.format("Partition %d :: SET lastSafeTxnId = %d",
                                             this.partitionId, this.lastSafeTxnId));
                }
            } // SYNCH
        }

        // this will update the state of the queue if needed
        this.checkQueueState(false);

        return this.lastSafeTxnId;
    }


    // ----------------------------------------------------------------------------
    // INTERNAL STATE CALCULATION
    // ----------------------------------------------------------------------------
    
    protected QueueState checkQueueState() {
        return this.checkQueueState(false);
    }
    
    /**
     * This is the most important method of the queue.
     * This will figure out the next state and how long we must wait until we 
     * can release the next transaction.
     * <B>Note:</B> I believe that this is the only thing that needs to be synchronized
     * @param afterPoll TODO
     * @return
     */
    private QueueState checkQueueState(boolean afterPoll) {
        if (trace.val) LOG.trace(String.format("Partition %d :: checkQueueState(afterPoll=%s) [current=%s]",
                                 this.partitionId, afterPoll, this.state));
        QueueState newState = (afterPoll ? QueueState.BLOCKED_SAFETY : QueueState.UNBLOCKED);
        long currentTimestamp = -1l;
        AbstractTransaction ts = super.peek();
        Long txnId = null;
        if (ts == null) {
            if (trace.val) LOG.trace(String.format("Partition %d :: Queue is empty.",
                                     this.partitionId));
            newState = QueueState.BLOCKED_EMPTY;
        }
        // Check whether can unblock now
        else {
            assert(ts.isInitialized()) :
                String.format("Unexpected uninitialized transaction %s [partition=%d]", ts, this.partitionId);
            txnId = ts.getTransactionId();
            assert(txnId != null) : "Null transaction id from " + txnId;
            
            // If this txnId is greater than the last safe one that we've seen, then we know
            // that the lastSafeTxnId has been polled. That means that we need to 
            // wait for an appropriate amount of time before we're allow to be executed.
            if (txnId.compareTo(this.lastSafeTxnId) > 0 && afterPoll == false) {
                newState = QueueState.BLOCKED_ORDERING;
                if (debug.val) LOG.debug(String.format("Partition %d :: txnId[%d] > lastSafeTxnId[%d]",
                                         this.partitionId, txnId, this.lastSafeTxnId));
            }
            // If our current block time is negative, then we know that we're the first txnId
            // that's been in the system. We'll also want to wait a bit before we're
            // allowed to be executed.
            else if (this.blockTimestamp == NULL_BLOCK_TIMESTAMP) {
                newState = QueueState.BLOCKED_SAFETY;
                if (debug.val) LOG.debug(String.format("Partition %d :: txnId[%d] ==> %s (blockTime=%d)",
                                         this.partitionId, txnId, newState, this.blockTimestamp));
            }
            // Check whether it's safe to unblock this mofo
            else if ((currentTimestamp = EstTime.currentTimeMillis()) < this.blockTimestamp) {
                newState = QueueState.BLOCKED_SAFETY;
                if (debug.val) LOG.debug(String.format("Partition %d :: txnId[%d] ==> %s (blockTime[%d] - current[%d] = %d)",
                                         this.partitionId, txnId, newState,
                                         this.blockTimestamp, EstTime.currentTimeMillis(),
                                         Math.max(0, this.blockTimestamp - EstTime.currentTimeMillis())));
            }
            // We didn't find any reason to block this txn, so it's sail yo for it...
            else if (debug.val) {
                LOG.debug(String.format("Partition %d :: Safe to Execute %d",
                          this.partitionId, txnId));
            }
        }
        
        if (newState != this.state) {
            synchronized (this) {
                if (newState != this.state) {
                    // note if we get non-empty but blocked
                    if ((newState == QueueState.BLOCKED_ORDERING) || (newState == QueueState.BLOCKED_SAFETY)) {
                        if (trace.val) LOG.trace(String.format("Partition %d :: NewState=%s --> %s",
                                                 this.partitionId, newState, ts));
                        long txnTimestamp = TransactionIdManager.getTimestampFromTransactionId(ts.getTransactionId().longValue());
                        if (currentTimestamp == -1) currentTimestamp = EstTime.currentTimeMillis();
                        
                        // Calculate how long we need to wait before this txn is safe to run
                        // If we're blocking on "safety", then we can use an offset based 
                        // on when the txnId was created. If we're blocking for "ordering",
                        // then we'll want to wait for the full wait time.
                        int waitTime = this.waitTime;
                        if (newState == QueueState.BLOCKED_SAFETY) {
                            waitTime = (int)Math.max(0, this.waitTime - (currentTimestamp - txnTimestamp));
                        }
                        
                        this.blockTimestamp = currentTimestamp + waitTime;
                        if (debug.val) LOG.debug(String.format("Partition %d :: SET blockTimestamp = %d --> %s",
                                                 this.partitionId, this.blockTimestamp, ts));
                        
                        newState = (waitTime > 0 ? QueueState.BLOCKED_SAFETY : QueueState.UNBLOCKED);
                        if (this.profiler != null) this.profiler.waitTimes.put(waitTime);
                        
                        // This txn becomes our next safeTxnId. This is essentially the next txn
                        // that should be executed, but somebody *could* come along and add in 
                        // a new txn with a lower id. But that's ok because we've synchronized setting
                        // the id up above. This is actually probably the only part of this entire method
                        // that needs to be protected...
                        this.lastSafeTxnId = txnId;
                        if (debug.val) LOG.debug(String.format("Partition %d :: SET lastSafeTxnId = %d --> %s",
                                                 this.partitionId, this.lastSafeTxnId, ts));
                        
                        if (debug.val) {
                            String debug = "";
                            if (trace.val) {
                                Map<String, Object> m = new LinkedHashMap<String, Object>();
                                m.put("Txn Init Timestamp", txnTimestamp);
                                m.put("Current Timestamp", currentTimestamp);
                                m.put("Block Time Remaining", (this.blockTimestamp - currentTimestamp));
                                debug = "\n" + StringUtil.formatMaps(m);
                            }
                            LOG.debug(String.format("Partition %d :: Blocking %s for %d ms " +
                            		  "[maxWait=%d, origState=%s, newState=%s]\n%s%s",
                                      this.partitionId, ts, (this.blockTimestamp - currentTimestamp),
                                      this.waitTime, this.state, newState, this.debug(), debug));
                        }
                    }
                    else if (newState == QueueState.UNBLOCKED) {
                        if (currentTimestamp == -1) currentTimestamp = EstTime.currentTimeMillis();
                        if (this.blockTimestamp > currentTimestamp) {
                            newState = QueueState.BLOCKED_SAFETY;
                        }
                    }
                    
                    // Set the new state
                    if (newState != this.state) {
                        if (debug.val) LOG.debug(String.format("Partition %d :: ORIG[%s]->NEW[%s] / LastSafeTxn:%d",
                                                 this.partitionId, this.state, newState, this.lastSafeTxnId));
                        if (this.profiler != null) {
                            this.profiler.queueStates.get(this.state).stopIfStarted();
                            this.profiler.queueStates.get(newState).start();
                        }
                        this.state = newState;
                    }
                }
            } // SYNCH
            // Sanity Check
            if ((this.state == QueueState.BLOCKED_ORDERING) || (this.state == QueueState.BLOCKED_SAFETY)) {
                assert(this.state != QueueState.BLOCKED_EMPTY);
            }
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
        public Long getLastTransactionId() {
            return (lastTxnPopped);
        }
        public long getBlockedTimestamp() {
            return (blockTimestamp);
        }
        public TransactionInitPriorityQueueProfiler getProfiler() {
            return (profiler);
        }
    }
    
    private TransactionInitPriorityQueue.Debug cachedDebugContext;
    public TransactionInitPriorityQueue.Debug getDebugContext() {
        if (cachedDebugContext == null) {
            // We don't care if we're thread-safe here...
            this.cachedDebugContext = new Debug();
        }
        return (cachedDebugContext);
    }
    
    public String debug() {
        long timestamp = EstTime.currentTimeMillis();
        @SuppressWarnings("unchecked")
        Map<String, Object> m[] = new Map[2];
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
        m[i].put("Peek Txn", super.peek());
        m[i].put("Wait Time", this.waitTime + " ms");
        m[i].put("Current Time", timestamp);
        m[i].put("Blocked Time", (this.blockTimestamp > 0 ? this.blockTimestamp + (this.blockTimestamp < timestamp ? " **PASSED**" : "") : "--"));
        m[i].put("Blocked Remaining", (this.blockTimestamp > 0 ? Math.max(0, this.blockTimestamp - timestamp) + " ms" : "--"));
        
        return (StringUtil.formatMaps(m));
    }
}
