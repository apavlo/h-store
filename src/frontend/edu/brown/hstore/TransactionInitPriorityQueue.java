package edu.brown.hstore;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.PriorityBlockingQueue;

import org.apache.log4j.Logger;
import org.voltdb.TransactionIdManager;
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
    
    private Long lastSeenTxn = null;
    private Long lastTxnPopped = null;
    private AbstractTransaction nextTxn = null;
    
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
     * Only return transaction state objects that are ready to run.
     */
    @Override
    public AbstractTransaction poll() {
        AbstractTransaction retval = null;
        
        // These invocations of poll() can return null if the next
        // txn was speculatively executed
        
        if (this.state == QueueState.BLOCKED_SAFETY) {
            if (EstTime.currentTimeMillis() >= this.blockTime) {
                retval = super.poll();
            }
        } else if (this.state == QueueState.UNBLOCKED) {
//            assert(checkQueueState() == QueueState.UNBLOCKED);
            retval = super.poll();
        }
        if (t) LOG.trace(String.format("Partition %d :: poll() -> %s", this.partitionId, retval));
        if (retval != null) {
            assert(this.nextTxn.equals(retval)) : 
                String.format("Partition %d :: Next txn is %s but our poll returned %s\n" +
                              StringUtil.SINGLE_LINE + "%s",
                              this.partitionId, this.nextTxn, retval, this.debug());
            this.nextTxn = null;
            this.lastTxnPopped = retval.getTransactionId();
        }
        
        if (d && retval != null)
            LOG.debug(String.format("Partition %d :: poll() -> %s", this.partitionId, retval));
        this.checkQueueState();
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
        
        // Check whether this new txn is less than the current this.nextTxn
        // If it is and there is still time remaining before it is released,
        // then we'll switch and become the new next this.nextTxn
        if (this.nextTxn != null && ts.compareTo(this.nextTxn) < 0) {
            this.checkQueueState();
            if (this.state != QueueState.UNBLOCKED) {
                if (d) LOG.debug(String.format("Partition %d :: Switching %s as new next txn [old=%s]",
                                 this.partitionId, ts, this.nextTxn));
                this.nextTxn = ts;
            }
            else {
                if (d) LOG.warn(String.format("Partition %d :: offer(%s) -> %s [next=%s]",
                                this.partitionId, ts, "REJECTED", this.nextTxn));
                return (false);
            }
        }
        
        boolean retval = super.offer(ts);
        if (d) LOG.debug(String.format("Partition %d :: offer(%s) -> %s",
                         this.partitionId, ts, retval));
        if (retval) this.checkQueueState();
        return retval;
    }

    @Override
    public boolean remove(Object obj) {
        AbstractTransaction ts = (AbstractTransaction)obj;
        boolean retval = super.remove(ts);
        boolean checkQueue = false;
        if (this.nextTxn != null && this.nextTxn == ts) {
            this.nextTxn = null;
            checkQueue = true;
            
        }
        // Sanity Check
        assert(super.contains(ts) == false) :
            "Failed to remove " + ts + "???\n" + this.debug();
        if (d) LOG.debug(String.format("Partition %d :: remove(%s) -> %s",
                         this.partitionId, ts, retval));
        if (checkQueue) this.checkQueueState();
        return retval;
    }

    /**
     * Update the information stored about the latest transaction
     * seen from each initiator. Compute the newest safe transaction id.
     */
    public Long noteTransactionRecievedAndReturnLastSeen(AbstractTransaction ts) {
        // this doesn't exclude dummy txnid but is also a sanity check
        assert(ts != null);

        // we've decided that this can happen, and it's fine... just ignore it
        if (d) {
            if (this.lastTxnPopped != null && this.lastTxnPopped.compareTo(ts.getTransactionId()) > 0) {
                LOG.warn(String.format("Txn ordering deadlock at Partition %d ::> LastTxn: %s / NewTxn: %s",
                                       this.partitionId, this.lastTxnPopped, ts));
                LOG.warn("LAST: " + this.lastTxnPopped);
                LOG.warn("NEW:  " + ts);
            }
        }

        // update the latest transaction for the specified initiator
        if (this.lastSeenTxn == null || ts.getTransactionId().compareTo(this.lastSeenTxn) < 0) {
            if (t) LOG.trace("SET lastSeenTxnId = " + ts);
            this.lastSeenTxn = ts.getTransactionId();
        }

        // this minimum is the newest safe transaction to run
        // but you still need to check if a transaction has been confirmed
        //  by its initiator
        //  (note: this check is done when peeking/polling from the queue)
        // if (t) LOG.trace("SET this.newestCandidateTransaction = " + this.lastSeenTxn);
        // this.newestCandidateTransaction = this.lastSeenTxn;

        // this will update the state of the queue if needed
        this.checkQueueState();

        // return the last seen id for the originating initiator
        return this.lastSeenTxn;
    }

    protected QueueState getQueueState() {
        return this.state;
    }
    
    protected int getPartitionId() {
        return (this.partitionId);
    }

    protected QueueState checkQueueState() {
        QueueState newState = QueueState.UNBLOCKED;
        AbstractTransaction ts = super.peek();
        if (ts == null) {
            if (t) LOG.trace(String.format("Partition %d :: Queue is empty.", this.partitionId));
            newState = QueueState.BLOCKED_EMPTY;
        }
        // Check whether can unblock now
        else if (ts == this.nextTxn && this.state != QueueState.UNBLOCKED) {
            if (EstTime.currentTimeMillis() < this.blockTime) {
                newState = QueueState.BLOCKED_SAFETY;
            }
            else if (d) {
                LOG.debug(String.format("Partition %d :: Wait time for %s has passed. Unblocking...",
                          this.partitionId, this.nextTxn));
            }
        }
        // This is a new txn and we should wait...
        else if (this.nextTxn != ts) {
            long txnTimestamp = TransactionIdManager.getTimestampFromTransactionId(ts.getTransactionId().longValue());
            long timestamp = EstTime.currentTimeMillis();
            long waitTime = Math.max(0, this.waitTime - (timestamp - txnTimestamp));
            newState = (waitTime > 0 ? QueueState.BLOCKED_SAFETY : QueueState.UNBLOCKED);
            this.blockTime = timestamp + waitTime;
            this.nextTxn = ts;
            
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
        
        if (newState != this.state) {
            this.state = newState;
            if (d) LOG.debug(String.format("Partition %d :: State:%s / NextTxn:%s",
                             this.partitionId, this.state, this.nextTxn));
        }
        return this.state;
    }
    
    public String debug() {
        Map<String, Object> m = new LinkedHashMap<String, Object>();
        m.put("PartitionId", this.partitionId);
        m.put("# of Elements", this.size());
        m.put("Wait Time", this.waitTime);
        m.put("Next Time Remaining", Math.max(0, EstTime.currentTimeMillis() - this.blockTime));
        m.put("Next Txn", this.nextTxn);
        m.put("Last Popped Txn", this.lastTxnPopped);
        m.put("Last Seen Txn", this.lastSeenTxn);
        return (StringUtil.formatMaps(m));
    }
}
