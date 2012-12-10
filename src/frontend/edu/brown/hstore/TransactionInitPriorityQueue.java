package edu.brown.hstore;

import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.PriorityBlockingQueue;

import org.apache.log4j.Logger;
import org.voltdb.TransactionIdManager;
import org.voltdb.utils.EstTime;

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
public class TransactionInitPriorityQueue extends PriorityBlockingQueue<Long> {
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
    
    private Long lastSeenTxnId = null;
    private Long lastTxnIdPopped = null;
    private Long nextTxnId = null;
    private final Set<Long> removed = new HashSet<Long>();
    
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
     * Only return transaction ids that are ready to run.
     */
    @Override
    public Long poll() {
        Long retval = null;
        while (true) {
            if (this.state == QueueState.UNBLOCKED ||
                (this.state == QueueState.BLOCKED_SAFETY && EstTime.currentTimeMillis() >= this.blockTime)) {
                retval = super.poll();
                if (retval != null && this.removed.contains(retval)) {
                    this.removed.remove(super.poll());
                    this.checkQueueState();
                    continue;
                }
            }
            break;
        } // WHILE
        
        if (t) LOG.trace(String.format("Partition %d :: poll() -> %s", this.partitionId, retval));
        if (retval != null) {
            assert(this.nextTxnId.equals(retval)) : 
                String.format("Partition %d :: Next txn is %s but our poll returned %s\n" +
                              StringUtil.SINGLE_LINE + "%s",
                              this.partitionId, this.nextTxnId, retval, this.debug());
            this.nextTxnId = null;
            this.lastTxnIdPopped = retval;
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
    public Long peek() {
        Long retval = null;
        if (this.state == QueueState.UNBLOCKED) {
            assert(checkQueueState() == QueueState.UNBLOCKED);
            retval = super.peek();
            assert(retval != null);
        }
        if (d) LOG.debug(String.format("Partition %d :: peek() -> %s", this.partitionId, retval));
        return retval;
    }
    
    @Override
    @Deprecated
    public boolean add(Long e) {
        return this.offer(e);
    }
    
    /**
     * Drop data for unknown initiators. This is the only valid add interface.
     */
    @Override
    public boolean offer(Long txnId) {
        assert(txnId != null);
        
        // Check whether this new txn is less than the current this.nextTxn
        // If it is and there is still time remaining before it is released,
        // then we'll switch and become the new next this.nextTxn
        if (this.nextTxnId != null && txnId.compareTo(this.nextTxnId) < 0) {
            this.checkQueueState();
            if (this.state != QueueState.UNBLOCKED) {
                if (d) LOG.debug(String.format("Partition %d :: Switching %s as new next txn [old=%s]",
                                 this.partitionId, txnId, this.nextTxnId));
                this.nextTxnId = txnId;
            }
            else {
                if (d) LOG.warn(String.format("Partition %d :: offer(%s) -> %s [next=%s]",
                                this.partitionId, txnId, "REJECTED", this.nextTxnId));
                return (false);
            }
        }
        
        boolean retval = super.offer(txnId);
        if (d) LOG.debug(String.format("Partition %d :: offer(%s) -> %s",
                         this.partitionId, txnId, retval));
        if (retval) this.checkQueueState();
        return retval;
    }

    @Override
    public boolean remove(Object obj) {
        Long txnId = (Long)obj;
        // 2012-12-10
        // Instead of immediately deleting the txnId from our queue, we're
        // just going to mark it as deleted and then clean it up later on...
        // boolean retval = super.remove(txnId);
        boolean retval = this.removed.add(txnId);
        boolean checkQueue = false;
        if (this.nextTxnId != null && this.nextTxnId == txnId) {
            this.nextTxnId = null;
            checkQueue = true;
        }
        // Sanity Check
//        assert(super.contains(txnId) == false) :
//            "Failed to remove " + txnId + "???\n" + this.debug();
        if (d) LOG.debug(String.format("Partition %d :: remove(%s) -> %s",
                         this.partitionId, txnId, retval));
        if (checkQueue) this.checkQueueState();
        return retval;
    }
    
    @Override
    public boolean contains(Object obj) {
        if (obj instanceof Long) {
            Long txnId = (Long)obj;
            return (this.removed.contains(txnId) ? false : super.contains(txnId));
        }
        return (false);
    }
    
    protected boolean cleanup(Long txnId) {
        // We'll just blindly delete from both. We have to make sure that
        // we delete it from our queue first before we delete it from our removed set
        boolean retval;
        retval = super.remove(txnId);
        retval = this.removed.remove(txnId) || retval;
        if (d) LOG.debug(String.format("Partition %d :: cleanup(%d) -> %s", this.partitionId, txnId, retval));
        return (retval);
    }

    /**
     * Update the information stored about the latest transaction
     * seen from each initiator. Compute the newest safe transaction id.
     */
    public Long noteTransactionRecievedAndReturnLastSeen(Long txnId) {
        // this doesn't exclude dummy txnid but is also a sanity check
        assert(txnId != null);

        // we've decided that this can happen, and it's fine... just ignore it
        if (d) {
            if (this.lastTxnIdPopped != null && this.lastTxnIdPopped.compareTo(txnId) > 0) {
                LOG.warn(String.format("Txn ordering deadlock at Partition %d ::> LastTxn: %s / NewTxn: %s",
                         this.partitionId, this.lastTxnIdPopped, txnId));
                LOG.warn("LAST: " + this.lastTxnIdPopped);
                LOG.warn("NEW:  " + txnId);
            }
        }

        // update the latest transaction for the specified initiator
        if (this.lastSeenTxnId == null || txnId.compareTo(this.lastSeenTxnId) < 0) {
            if (t) LOG.trace("SET lastSeenTxnId = " + txnId);
            this.lastSeenTxnId = txnId;
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
        return this.lastSeenTxnId;
    }

    protected QueueState getQueueState() {
        return this.state;
    }
    
    protected int getPartitionId() {
        return (this.partitionId);
    }

    protected QueueState checkQueueState() {
        QueueState newState = QueueState.UNBLOCKED;
        Long txnId = null;
        while (true) {
            txnId = super.peek();
            if (txnId != null && this.removed.contains(txnId)) {
                this.removed.remove(super.poll());
                continue;
            }
            break;
        } // WHILE
        
        if (txnId == null) {
            if (t) LOG.trace(String.format("Partition %d :: Queue is empty.", this.partitionId));
            newState = QueueState.BLOCKED_EMPTY;
        }
        // Check whether can unblock now
        else if (txnId == this.nextTxnId && this.state != QueueState.UNBLOCKED) {
            if (EstTime.currentTimeMillis() < this.blockTime) {
                newState = QueueState.BLOCKED_SAFETY;
            }
            else if (d) {
                LOG.debug(String.format("Partition %d :: Wait time for %s has passed. Unblocking...",
                          this.partitionId, this.nextTxnId));
            }
        }
        // This is a new txn and we should wait...
        else if (this.nextTxnId != txnId) {
            long txnTimestamp = TransactionIdManager.getTimestampFromTransactionId(txnId.longValue());
            long timestamp = EstTime.currentTimeMillis();
            long waitTime = Math.max(0, this.waitTime - (timestamp - txnTimestamp));
            newState = (waitTime > 0 ? QueueState.BLOCKED_SAFETY : QueueState.UNBLOCKED);
            this.blockTime = timestamp + waitTime;
            this.nextTxnId = txnId;
            
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
                          this.partitionId, txnId, (this.blockTime - timestamp),
                          this.waitTime, debug));
            }
        }
        
        if (newState != this.state) {
            this.state = newState;
            if (d) LOG.debug(String.format("Partition %d :: State:%s / NextTxn:%s",
                             this.partitionId, this.state, this.nextTxnId));
        }
        return this.state;
    }
    
    @Override
    public Iterator<Long> iterator() {
        final Iterator<Long> superIt = super.iterator();
        return new Iterator<Long>() {
            private Long next = null;
            private final Iterator<Long> it = superIt;
            
            @Override
            public void remove() {
                if (this.next != null) {
                    TransactionInitPriorityQueue.this.remove(this.next);
                }
            }
            @Override
            public Long next() {
                return (this.next);
            }
            @Override
            public boolean hasNext() {
                Long txnId = null;
                this.next = null;
                while (this.it.hasNext()) {
                    txnId = this.it.next();
                    if (TransactionInitPriorityQueue.this.removed.contains(txnId)) {
                        continue;
                    }
                    this.next = txnId;
                    break;
                } // WHILE
                return (this.next != null);
            }
        };
    }
    
    public String debug() {
        Map<String, Object> m = new LinkedHashMap<String, Object>();
        m.put("PartitionId", this.partitionId);
        m.put("# of Elements", this.size());
        m.put("Wait Time", this.waitTime);
        m.put("Next Time Remaining", Math.max(0, EstTime.currentTimeMillis() - this.blockTime));
        m.put("Next Txn", this.nextTxnId);
        m.put("Last Popped Txn", this.lastTxnIdPopped);
        m.put("Last Seen Txn", this.lastSeenTxnId);
        return (StringUtil.formatMaps(m));
    }
}
