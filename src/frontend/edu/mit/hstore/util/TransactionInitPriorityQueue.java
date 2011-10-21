package edu.mit.hstore.util;

import java.util.concurrent.PriorityBlockingQueue;

import org.apache.log4j.Logger;
import org.voltdb.TransactionIdManager;

import edu.brown.utils.LoggerUtil;
import edu.brown.utils.LoggerUtil.LoggerBoolean;
import edu.mit.hstore.HStoreSite;

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
public class TransactionInitPriorityQueue extends ThrottlingQueue<Long> {
    private static final Logger LOG = Logger.getLogger(TransactionInitPriorityQueue.class);
    private final static LoggerBoolean debug = new LoggerBoolean(LOG.isDebugEnabled());
    private final static LoggerBoolean trace = new LoggerBoolean(LOG.isTraceEnabled());
    static {
        LoggerUtil.attachObserver(LOG, debug, trace);
    }
    
    private static final long serialVersionUID = 1L;

    public enum QueueState {
        UNBLOCKED,
        BLOCKED_EMPTY,
        BLOCKED_ORDERING,
        BLOCKED_SAFETY;
    }

    /**
     * Not using this class because all we care about is txn_ids, not the sites themselves.
     */
    long m_lastSeenTxnId;
    long m_lastSafeTxnId;

    long m_newestCandidateTransaction = -1;
    final int m_siteId;
    final int m_partitionId;
    long m_txnsPopped = 0;
    long m_lastTxnPopped = 0;
    long m_blockTime = 0;
    Long m_nextTxn = null;
    final long m_waitTime;
    QueueState m_state = QueueState.BLOCKED_EMPTY;

    /**
     * Tell this queue about all initiators. If any initiators
     * are later referenced that aren't in this list, trip
     * an assertion.
     * @param partitionId TODO
     */
    public TransactionInitPriorityQueue(HStoreSite hstore_site, int partitionId, long wait) {
        super(new PriorityBlockingQueue<Long>(),
              hstore_site.getHStoreConf().site.queue_dtxn_max_per_partition,
              hstore_site.getHStoreConf().site.queue_dtxn_release_factor,
              hstore_site.getHStoreConf().site.queue_dtxn_increase);
        m_siteId = hstore_site.getSiteId();
        m_partitionId = partitionId;
        m_waitTime = wait;
    }

    /**
     * Only return transaction state objects that are ready to run.
     */
    @Override
    public synchronized Long poll() {
        Long retval = null;
        if (m_state == QueueState.BLOCKED_SAFETY) {
            if (System.currentTimeMillis() >= m_blockTime) {
                retval = super.poll();
                assert(retval != null);
            }
        } else if (m_state == QueueState.UNBLOCKED) {
            assert(checkQueueState() == QueueState.UNBLOCKED);
            retval = super.poll();
            assert(retval != null);
        }
        if (retval != null) {
            assert(m_nextTxn == retval);
            m_nextTxn = null;
            
            // call this again to check
            checkQueueState();
            m_txnsPopped++;
            m_lastTxnPopped = retval;
        }
        return retval;
    }

    /**
     * Only return transaction state objects that are ready to run.
     */
    @Override
    public Long peek() {
        Long retval = null;
        if (m_state == QueueState.UNBLOCKED) {
            assert(checkQueueState() == QueueState.UNBLOCKED);
            retval = super.peek();
            assert(retval != null);
        }
        return retval;
    }

    /**
     * Drop data for unknown initiators. This is the only valid add interface.
     */
    @Override
    public synchronized boolean offer(Long txnID, boolean force) {
        assert(txnID != null);
        boolean retval = super.offer(txnID, force);
        // update the queue state
        if (retval) checkQueueState();
        return retval;
    }

    @Override
    public boolean remove(Object txnID) {
        boolean retval = super.remove(txnID);
        checkQueueState();
        return retval;
    }

    /**
     * Update the information stored about the latest transaction
     * seen from each initiator. Compute the newest safe transaction id.
     */
    public long noteTransactionRecievedAndReturnLastSeen(long txnId)
    {
        // this doesn't exclude dummy txnid but is also a sanity check
        assert(txnId != 0);

        // we've decided that this can happen, and it's fine... just ignore it
        if (m_lastTxnPopped > txnId) {
            LOG.warn(String.format("Txn ordering deadlock at partition %d -> LastTxn: %d / NewTxn: %d",
                                   m_partitionId, m_lastTxnPopped, txnId));
            LOG.warn("LAST: " + TransactionIdManager.toString(m_lastTxnPopped));
            LOG.warn("NEW:  " + TransactionIdManager.toString(txnId));
        }

        // update the latest transaction for the specified initiator
        if (m_lastSeenTxnId < txnId)
            m_lastSeenTxnId = txnId;

        // this minimum is the newest safe transaction to run
        // but you still need to check if a transaction has been confirmed
        //  by its initiator
        //  (note: this check is done when peeking/polling from the queue)
        m_newestCandidateTransaction = m_lastSeenTxnId;

        // this will update the state of the queue if needed
        checkQueueState();

        // return the last seen id for the originating initiator
        return m_lastSeenTxnId;
    }

    /**
     * Remove all pending transactions from the specified initiator
     * and do not require heartbeats from that initiator to proceed.
     * @param initiatorId id of the failed initiator.
     */
    public void gotFaultForInitiator(int initiatorId) {
        // calculate the next minimum transaction w/o our dead friend
        noteTransactionRecievedAndReturnLastSeen(Long.MAX_VALUE);
    }

    public void faultTransaction(Long txnID) {
        this.remove(txnID);
    }

    /**
     * @return The id of the newest safe transaction to run.
     */
    long getNewestSafeTransaction() {
        return m_newestCandidateTransaction;
    }

    public void shutdown() throws InterruptedException {
    }

    public QueueState getQueueState() {
        return m_state;
    }

    private synchronized QueueState checkQueueState() {
        QueueState newState = QueueState.UNBLOCKED;
        Long ts = super.peek();
        long now = System.currentTimeMillis();
        if (ts == null) {
            if (debug.get()) LOG.debug(String.format("Partition %d - Queue is empty.", m_partitionId));
            newState = QueueState.BLOCKED_EMPTY;
        }
        // Check whether can unblock now
        else if (ts == m_nextTxn && now >= m_blockTime) {
            if (debug.get()) LOG.debug(String.format("Partition %d - Wait time for txn #%d has passed. Unblocking...", m_partitionId, m_nextTxn));
            newState = QueueState.UNBLOCKED;
        }
        // This is a new txn and we should wait...
        else if (m_nextTxn == null || m_nextTxn != ts) {
            if (debug.get()) LOG.debug(String.format("Partition %d - Blocking next txn #%d for %d ms", m_partitionId, ts, m_waitTime));
            newState = QueueState.BLOCKED_SAFETY;
            m_blockTime = now + this.m_waitTime;
            m_nextTxn = ts;
        }
        
        if (newState != m_state) {
            m_state = newState;
            if (debug.get()) LOG.debug(String.format("Partition %d - State:%s / NextTxn:%s", m_partitionId, m_state, m_nextTxn));
        }
        return m_state;
    }
}
