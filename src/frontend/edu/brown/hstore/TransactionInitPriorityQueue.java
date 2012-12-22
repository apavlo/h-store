package edu.brown.hstore;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.concurrent.PriorityBlockingQueue;

import org.apache.log4j.Logger;
import org.voltdb.TransactionIdManager;
import org.voltdb.dtxn.RestrictedPriorityQueue.QueueState;
import org.voltdb.utils.EstTime;

import edu.brown.hstore.txns.AbstractTransaction;
import edu.brown.logging.LoggerUtil;
import edu.brown.logging.LoggerUtil.LoggerBoolean;
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
    private static final Logger LOG = Logger.getLogger(TransactionInitPriorityQueue.class);
    private static final LoggerBoolean debug = new LoggerBoolean(LOG.isDebugEnabled());
    private static final LoggerBoolean trace = new LoggerBoolean(LOG.isTraceEnabled());
    static {
        LoggerUtil.attachObserver(LOG, debug, trace);
    }
    private static boolean d = debug.get();
    private static boolean t = trace.get();
    
    public enum QueueState {
        UNBLOCKED,
        BLOCKED_EMPTY,
        BLOCKED_ORDERING,
        BLOCKED_SAFETY;
    }

    private final int partitionId;
    private final long waitTime;
    
    private long blockTime = -1;
    private QueueState state = QueueState.BLOCKED_EMPTY;
    private long txnsPopped = 0;
    
    private Long lastSeenTxnId = null;
    private Long lastSafeTxnId = null;
    private Long lastTxnPopped = null;
    
    // private AbstractTransaction nextTxn = null;
    
    /**
     * Constructor
     * @param hstore_site
     * @param partitionId
     * @param wait
     */
    public TransactionInitPriorityQueue(HStoreSite hstore_site, int partitionId, long wait) {
        super();
        this.partitionId = partitionId;
        this.waitTime = wait;
    }
    
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
    
    protected long getTransactionsPopped() {
        return (this.txnsPopped);
    }
    
    protected Long getLastTransactionId() {
        return (this.lastTxnPopped);
    }

    /**
     * Only return transaction state objects that are ready to run.
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
                this.txnsPopped++;
                this.lastTxnPopped = retval.getTransactionId();
            }
            // call this again to prime the next txn
            this.checkQueueState();
        }
        if (d && retval != null)
            LOG.debug(String.format("Partition %d :: poll() -> %s", this.partitionId, retval));
        return retval;
    }

    /**
     * Only return transaction state objects that are ready to run.
     */
    @Override
    public AbstractTransaction peek() {
        AbstractTransaction retval = null;
        if (this.state == QueueState.UNBLOCKED) {
            assert(checkQueueState() == QueueState.UNBLOCKED);
            retval = super.peek();
            assert(retval != null);
        }
        if (d) LOG.debug(String.format("Partition %d :: peek() -> %s", this.partitionId, retval));
        return retval;
    }
    
    /**
     * Drop data for unknown initiators. This is the only valid add interface.
     */
    @Override
    public boolean offer(AbstractTransaction ts) {
        assert(ts != null);
        assert(ts.isInitialized());
        
        boolean retval = super.offer(ts);
        // update the queue state
        if (d) LOG.debug(String.format("Partition %d :: offer(%s) -> %s",
                         this.partitionId, ts, retval));
        if (retval) this.checkQueueState();
        return retval;
    }

    @Override
    public boolean remove(Object obj) {
        AbstractTransaction txn = (AbstractTransaction)obj;
        boolean retval = super.remove(txn);
        // Sanity Check
        assert(super.contains(txn) == false) :
            "Failed to remove " + txn + "???\n" + this.debug();
        if (d) LOG.warn(String.format("Partition %d :: remove(%s) -> %s",
                        this.partitionId, txn, retval));
        this.checkQueueState();
        return retval;
    }
    
    /**
     * Update the information stored about the latest transaction
     * seen from each initiator. Compute the newest safe transaction id.
     */
    public Long noteTransactionRecievedAndReturnLastSeen(Long txnId) {
        assert(txnId != null);

        // we've decided that this can happen, and it's fine... just ignore it
        if (d) {
            if (this.lastTxnPopped != null && this.lastTxnPopped.compareTo(txnId) > 0) {
                LOG.warn(String.format("Txn ordering deadlock at Partition %d ::> LastTxn: %d / NewTxn: %d",
                                       this.partitionId, this.lastTxnPopped, txnId));
                LOG.warn("LAST: " + this.lastTxnPopped);
                LOG.warn("NEW:  " + txnId);
            }
        }

        this.lastSeenTxnId = txnId;
        if (d) LOG.debug(String.format("Partition %d :: SET lastSeenTxnId = %d",
                         this.partitionId, this.lastSeenTxnId));
        if (this.lastSafeTxnId == null || txnId.compareTo(this.lastSafeTxnId) < 0) {
            this.lastSafeTxnId = txnId;
            if (d) LOG.debug(String.format("Partition %d :: SET lastSafeTxnId = %d",
                             this.partitionId, this.lastSafeTxnId));
        }

        // this will update the state of the queue if needed
        this.checkQueueState();

        return this.lastSafeTxnId;
    }

    protected synchronized QueueState checkQueueState() {
        QueueState newState = QueueState.UNBLOCKED;
        AbstractTransaction ts = super.peek();
        Long txnId = null;
        if (ts == null) {
            if (t) LOG.trace(String.format("Partition %d :: Queue is empty.", this.partitionId));
            newState = QueueState.BLOCKED_EMPTY;
        }
        // Check whether can unblock now
        else {
            assert(ts.isInitialized());
            txnId = ts.getTransactionId();
            
            // If this txnId is greater than the last safe one that we've seen, then we know
            // that the lastSafeTxnId has been polled. That means that we need to 
            // wait for an appropriate amount of time before we're allow to be executed.
            if (txnId.compareTo(this.lastSafeTxnId) > 0) {
                newState = QueueState.BLOCKED_SAFETY;
                if (d) LOG.debug(String.format("Partition %d :: txnId[%d] > lastSafeTxnId[%d]",
                                 this.partitionId, txnId, this.lastSafeTxnId));
            }
            // If our current state is empty, then we know that we're the first txnId
            // that's been in the system. We'll also want to wait a bit before we're
            // allowed to be executed.
            else if (this.blockTime == -1) {
                newState = QueueState.BLOCKED_SAFETY;
                if (d) LOG.debug(String.format("Partition %d :: txnId[%d] ==> %s",
                                 this.partitionId, txnId, this.state));
            }
            
            if (d) LOG.debug(String.format("Partition %d :: NewState=%s\n%s", this.partitionId, newState, this.debug()));
        }
        if (newState != this.state) {
            // note if we get non-empty but blocked
            if ((newState == QueueState.BLOCKED_ORDERING) || (newState == QueueState.BLOCKED_SAFETY)) {
                long txnTimestamp = TransactionIdManager.getTimestampFromTransactionId(ts.getTransactionId().longValue());
                long timestamp = EstTime.currentTimeMillis();
                long waitTime = Math.max(0, this.waitTime - (timestamp - txnTimestamp));
                this.blockTime = timestamp + waitTime;
                newState = (waitTime > 0 ? QueueState.BLOCKED_SAFETY : QueueState.UNBLOCKED);
                if (this.lastTxnPopped != null && this.lastTxnPopped.equals(this.lastSafeTxnId)) {
                    this.lastSafeTxnId = txnId;
                    if (d) LOG.debug(String.format("Partition %d :: SET lastSafeTxnId = %d",
                                     this.partitionId, this.lastSafeTxnId));
                }
                
                if (d) {
                    String debug = "";
                    if (t) {
                        Map<String, Object> m = new LinkedHashMap<String, Object>();
                        m.put("Txn Init Timestamp", txnTimestamp);
                        m.put("Current Timestamp", timestamp);
                        m.put("Block Time Remaining", (this.blockTime - timestamp));
                        debug = "\n" + StringUtil.formatMaps(m);
                    }
                    LOG.debug(String.format("Partition %d :: Blocking %s for %d ms " +
                    		  "[maxWait=%d, origState=%s, newState=%s]\n%s%s",
                              this.partitionId, ts, (this.blockTime - timestamp),
                              this.waitTime, this.state, newState, this.debug(), debug));
                }
            }
            else if (newState == QueueState.UNBLOCKED) {
                if (this.blockTime > EstTime.currentTimeMillis()) {
                    newState = QueueState.BLOCKED_SAFETY;
                }
            }
            
            // Sanity Check
            if ((this.state == QueueState.BLOCKED_ORDERING) || (this.state == QueueState.BLOCKED_SAFETY)) {
                assert(this.state != QueueState.BLOCKED_EMPTY);
            }
        }
        if (newState != this.state) {
            if (d) LOG.debug(String.format("Partition %d :: ORIG[%s]->NEW[%s] / LastSafeTxn:%d",
                             this.partitionId, this.state, newState, this.lastSafeTxnId));
            this.state = newState;
        }
        return this.state;
    }
    
    public String debug() {
        Map<String, Object> m = new LinkedHashMap<String, Object>();
        m.put("PartitionId", this.partitionId);
        m.put("Current State", this.state);
        m.put("# of Elements", this.size());
        m.put("Wait Time", this.waitTime);
        m.put("Next Time Remaining", Math.max(0, EstTime.currentTimeMillis() - this.blockTime));
        m.put("Peek Txn", super.peek());
        m.put("Last Popped Txn", this.lastTxnPopped);
        m.put("Last Seen Txn", this.lastSeenTxnId);
        m.put("Last Safe Txn", this.lastSafeTxnId);
        return (StringUtil.formatMaps(m));
    }
}
