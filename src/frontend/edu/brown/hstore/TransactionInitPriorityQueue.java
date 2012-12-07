package edu.brown.hstore;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.PriorityBlockingQueue;

import org.apache.commons.collections15.buffer.CircularFifoBuffer;
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

    final int m_siteId;
    final int m_partitionId;
    final long m_waitTime;
    
    long m_txnsPopped = 0;
    long m_blockTime = 0;
    QueueState m_state = QueueState.BLOCKED_EMPTY;
    
    Long m_lastSeenTxn = null;
    AbstractTransaction m_newestCandidateTransaction = null;
    AbstractTransaction m_lastTxnPopped = null;
    AbstractTransaction m_nextTxn = null;
    
    final CircularFifoBuffer<String> lastRemoved = new CircularFifoBuffer<String>(10);
    final CircularFifoBuffer<String> lastPolled = new CircularFifoBuffer<String>(10);
    
    /**
     * Constructor
     * @param hstore_site
     * @param partitionId
     * @param wait
     */
    public TransactionInitPriorityQueue(HStoreSite hstore_site, int partitionId, long wait) {
        super();
        m_siteId = hstore_site.getSiteId();
        m_partitionId = partitionId;
        m_waitTime = wait;
    }

    /**
     * Only return transaction state objects that are ready to run.
     */
    @Override
    public synchronized AbstractTransaction poll() {
        AbstractTransaction retval = null;
        
        // These invocations of poll() can return null if the next
        // txn was speculatively executed
        
        if (m_state == QueueState.BLOCKED_SAFETY) {
            if (EstTime.currentTimeMillis() >= m_blockTime) {
                retval = super.poll();
            }
        } else if (m_state == QueueState.UNBLOCKED) {
//            assert(checkQueueState() == QueueState.UNBLOCKED);
            retval = super.poll();
        }
        if (t) LOG.trace(String.format("Partition %d poll() -> %s", m_partitionId, retval));
        if (retval != null) {
            assert(m_nextTxn.equals(retval)) : 
                String.format("Partition %d - Next txn is %s but our poll returned %s\n" +
                              StringUtil.SINGLE_LINE +
                		      "%s\n" +
                		      StringUtil.SINGLE_LINE +
                		      "%s",
                              m_partitionId, m_nextTxn, retval, this.debug(),
                              StringUtil.join("\n", super.iterator()));
            m_nextTxn = null;
            m_txnsPopped++;
            m_lastTxnPopped = retval;
            this.lastPolled.add(retval.toString());
        }
        this.checkQueueState();
        
         if (d && retval != null)
            LOG.debug(String.format("Partition %d poll() -> %s", m_partitionId, retval));
        return retval;
    }

    /**
     * Only return transaction state objects that are ready to run.
     */
    @Override
    public synchronized AbstractTransaction peek() {
        AbstractTransaction retval = null;
        if (m_state == QueueState.UNBLOCKED) {
            assert(checkQueueState() == QueueState.UNBLOCKED);
            retval = super.peek();
            assert(retval != null);
        }
        if (d) LOG.debug(String.format("Partition %d peek() -> %s", m_partitionId, retval));
        return retval;
    }

    /**
     * Drop data for unknown initiators. This is the only valid add interface.
     */
    @Override
    public synchronized boolean offer(AbstractTransaction txnId) {
        assert(txnId != null);
        
        // Check whether this new txn is less than the current m_nextTxn
        // If it is and there is still time remaining before it is released,
        // then we'll switch and become the new next m_nextTxn
        if (m_nextTxn != null && txnId.compareTo(m_nextTxn) < 0) {
            checkQueueState();
            if (m_state != QueueState.UNBLOCKED) {
                if (d) LOG.debug(String.format("Partition %d Switching %s as new next txn [old=%s]",
                                 m_partitionId, txnId, m_nextTxn));
                m_nextTxn = txnId;
            } else {
                if (d) LOG.debug(String.format("Partition %d offer(%s) -> %s",
                                 m_partitionId, txnId, "REJECTED"));
                return (false);
            }
        }
        
        boolean retval = super.offer(txnId);
        // update the queue state
        // if (retval) checkQueueState();
        this.checkQueueState();
        if (d) LOG.debug(String.format("Partition %d offer(%s) -> %s",
                         m_partitionId, txnId, retval));
        return retval;
    }

    @Override
    public synchronized boolean remove(Object obj) {
        AbstractTransaction ts = (AbstractTransaction)obj;
        boolean retval = super.remove(ts);
        if (retval) this.lastRemoved.add(ts.toString());
        if (m_nextTxn != null && m_nextTxn == ts) {
            m_nextTxn = null;
        }
        this.checkQueueState();
        if (d) LOG.debug(String.format("Partition %d remove(%s) -> %s",
                         m_partitionId, ts, retval));
        // Sanity Check
        assert(super.contains(ts) == false) :
            "Failed to remove " + ts + "???\n" + this.debug();
        return retval;
    }

    /**
     * Update the information stored about the latest transaction
     * seen from each initiator. Compute the newest safe transaction id.
     */
    public synchronized Long noteTransactionRecievedAndReturnLastSeen(AbstractTransaction txn) {
        // this doesn't exclude dummy txnid but is also a sanity check
        assert(txn != null);

        // we've decided that this can happen, and it's fine... just ignore it
        if (m_lastTxnPopped != null && m_lastTxnPopped.compareTo(txn) > 0) {
            if (d) {
                LOG.warn(String.format("Txn ordering deadlock at partition %d -> LastTxn: %s / NewTxn: %s",
                                       m_partitionId, m_lastTxnPopped, txn));
                LOG.warn("LAST: " + m_lastTxnPopped);
                LOG.warn("NEW:  " + txn);
            }
        }

        // update the latest transaction for the specified initiator
        if (m_lastSeenTxn == null || txn.getTransactionId().compareTo(m_lastSeenTxn) < 0) {
            if (t) LOG.trace("SET lastSeenTxnId = " + txn);
            m_lastSeenTxn = txn.getTransactionId();
        }

        // this minimum is the newest safe transaction to run
        // but you still need to check if a transaction has been confirmed
        //  by its initiator
        //  (note: this check is done when peeking/polling from the queue)
        // if (t) LOG.trace("SET m_newestCandidateTransaction = " + m_lastSeenTxn);
        // m_newestCandidateTransaction = m_lastSeenTxn;

        // this will update the state of the queue if needed
        checkQueueState();

        // return the last seen id for the originating initiator
        return m_lastSeenTxn;
    }

    public void faultTransaction(Long txnID) {
        this.remove(txnID);
    }

    public void shutdown() throws InterruptedException {
    }

    public QueueState getQueueState() {
        return m_state;
    }
    
    public int getPartitionId() {
        return (this.m_partitionId);
    }

    protected QueueState checkQueueState() {
        QueueState newState = QueueState.UNBLOCKED;
        AbstractTransaction ts = super.peek();
        if (ts == null) {
            if (d) LOG.debug(String.format("Partition %d - Queue is empty.", m_partitionId));
            newState = QueueState.BLOCKED_EMPTY;
        }
        // Check whether can unblock now
        else if (ts == m_nextTxn && m_state != QueueState.UNBLOCKED) {
            if (EstTime.currentTimeMillis() < m_blockTime) {
                newState = QueueState.BLOCKED_SAFETY;
            } else if (d) {
                LOG.debug(String.format("Partition %d - Wait time for %s has passed. Unblocking...",
                          m_partitionId, m_nextTxn));
            }
        }
        // This is a new txn and we should wait...
        else if (m_nextTxn == null || m_nextTxn != ts) {
            newState = QueueState.BLOCKED_SAFETY;
            // m_blockTime = EstTime.currentTimeMillis() + this.m_waitTime;
            long txnTimestamp = TransactionIdManager.getTimestampFromTransactionId(ts.getTransactionId().longValue());
            long timestamp = EstTime.currentTimeMillis();
            m_blockTime = timestamp + Math.max(0, this.m_waitTime - (timestamp - txnTimestamp));
            m_nextTxn = ts;
            if (d) {
                String debug = "";
                if (t) {
                    Map<String, Object> m = new LinkedHashMap<String, Object>();
                    m.put("Txn Init Timestamp", txnTimestamp);
                    m.put("Current Timestamp", timestamp);
                    m.put("Block Time Remaining", (m_blockTime - timestamp));
                    debug = "\n" + StringUtil.formatMaps(m);
                }
                LOG.debug(String.format("Partition %d - Blocking next %s for %d ms [defaultWait=%d]%s",
                          m_partitionId, ts, (m_blockTime - timestamp), m_waitTime, debug));
            }
        }
        
        if (newState != m_state) {
            m_state = newState;
            if (d) LOG.debug(String.format("Partition %d - State:%s / NextTxn:%s",
                             m_partitionId, m_state, m_nextTxn));
        }
        return m_state;
    }
    
    public synchronized String debug() {
        Map<String, Object> m = new LinkedHashMap<String, Object>();
        m.put("PartitionId", m_partitionId);
        m.put("# of Elements", this.size());
        m.put("Wait Time", m_waitTime);
        m.put("Next Time Remaining", Math.max(0, EstTime.currentTimeMillis() - m_blockTime));
        m.put("Next Txn", m_nextTxn);
        m.put("Last Popped Txn", m_lastTxnPopped);
        m.put("Last Seen Txn", m_lastSeenTxn);
        m.put("Last Polled", StringUtil.join("\n", this.lastPolled));
        m.put("Last Removed", StringUtil.join("\n", this.lastRemoved));
        return (StringUtil.formatMaps(m));
    }
}
