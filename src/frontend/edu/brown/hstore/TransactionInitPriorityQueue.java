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
    
    private long blockTime = 0;
    private QueueState state = QueueState.BLOCKED_EMPTY;
    private long txnsPopped = 0;
    
    private Long newestCandidateTransaction = null;
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
    

    protected QueueState getQueueState() {
        return this.state;
    }
    
    protected int getPartitionId() {
        return (this.partitionId);
    }

    /**
     * Only return transaction state objects that are ready to run.
     */
    @Override
    public AbstractTransaction poll() {
        AbstractTransaction retval = null;
        if (this.state == QueueState.UNBLOCKED || 
            (this.state == QueueState.BLOCKED_SAFETY && this.blockTime < EstTime.currentTimeMillis())) {
            assert(checkQueueState() == QueueState.UNBLOCKED);
            retval = super.poll();
            assert(retval != null);
            this.txnsPopped++;
            this.lastTxnPopped = retval.getTransactionId();
            // call this again to check
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

        if (this.lastSeenTxnId == null || txnId.compareTo(this.lastSeenTxnId) < 0) {
            if (t) LOG.trace("SET lastSeenTxnId = " + txnId);
            this.lastSeenTxnId = txnId;
        }
        if (this.lastSafeTxnId == null || txnId.compareTo(this.lastSafeTxnId) < 0) {
            if (t) LOG.trace("SET lastSafeTxnId = " + txnId);
            this.lastSafeTxnId = txnId;
        }

        // this minimum is the newest safe transaction to run
        // but you still need to check if a transaction has been confirmed
        //  by its initiator
        //  (note: this check is done when peeking/polling from the queue)
        if (t) LOG.trace("SET this.newestCandidateTransaction = " + this.lastSeenTxnId);
        this.newestCandidateTransaction = this.lastSeenTxnId;

        // this will update the state of the queue if needed
        this.checkQueueState();

        // return the last seen id for the originating initiator
        return this.lastSeenTxnId;
    }

    protected QueueState checkQueueState() {
        QueueState newState = QueueState.UNBLOCKED;
        AbstractTransaction ts = super.peek();
        if (ts == null) {
            if (t) LOG.trace(String.format("Partition %d :: Queue is empty.", this.partitionId));
            newState = QueueState.BLOCKED_EMPTY;
        }
        // Check whether can unblock now
        else {
            assert(ts.isInitialized());
            
            Long txnId = ts.getTransactionId();
            if (txnId.compareTo(this.newestCandidateTransaction) > 0) {
                newState = QueueState.BLOCKED_ORDERING;
            }
            else if (txnId.compareTo(this.lastSafeTxnId) > 0) {
                newState = QueueState.BLOCKED_SAFETY;
            }
            if (t) LOG.trace(String.format("Partition %d :: NewState=%s\n%s", this.partitionId, newState, this.debug()));
        }
        if (newState != this.state) {
            // note if we get non-empty but blocked
            if ((newState == QueueState.BLOCKED_ORDERING) || (newState == QueueState.BLOCKED_SAFETY)) {
                long txnTimestamp = TransactionIdManager.getTimestampFromTransactionId(ts.getTransactionId().longValue());
                long timestamp = EstTime.currentTimeMillis();
                long waitTime = Math.max(0, this.waitTime - (timestamp - txnTimestamp));
                newState = (waitTime > 0 ? QueueState.BLOCKED_SAFETY : QueueState.UNBLOCKED);
                this.blockTime = timestamp + waitTime;
                
                if (d) {
                    String debug = "";
                    if (t) {
                        Map<String, Object> m = new LinkedHashMap<String, Object>();
                        m.put("Txn Init Timestamp", txnTimestamp);
                        m.put("Current Timestamp", timestamp);
                        m.put("Block Time Remaining", (this.blockTime - timestamp));
                        debug = "\n" + StringUtil.formatMaps(m);
                    }
                    LOG.debug(String.format("Partition %d :: Blocking %s for %d ms [maxWait=%d]%s",
                              this.partitionId, ts, (this.blockTime - timestamp),
                              this.waitTime, debug));
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
            this.state = newState;
            if (d) LOG.debug(String.format("Partition %d :: State:%s / LastSafeTxn:%d",
                             this.partitionId, this.state, this.lastSafeTxnId));
        }
        return this.state;
    }
    
    public String debug() {
        Map<String, Object> m = new LinkedHashMap<String, Object>();
        m.put("PartitionId", this.partitionId);
        m.put("# of Elements", this.size());
        m.put("Wait Time", this.waitTime);
        m.put("Next Time Remaining", Math.max(0, EstTime.currentTimeMillis() - this.blockTime));
        m.put("Last Popped Txn", this.lastTxnPopped);
        m.put("Last Seen Txn", this.lastSeenTxnId);
        m.put("Last Safe Txn", this.lastSafeTxnId);
        m.put("Newest Candidate", this.newestCandidateTransaction);
        return (StringUtil.formatMaps(m));
    }
}
