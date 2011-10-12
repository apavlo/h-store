package edu.mit.hstore.util;

import java.util.PriorityQueue;

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
public class TransactionInitPriorityQueue extends PriorityQueue<Long> {
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
    long m_txnsPopped = 0;
    long m_lastTxnPopped = 0;
    long m_blockTime = 0;
    QueueState m_state = QueueState.BLOCKED_EMPTY;

    /**
     * Tell this queue about all initiators. If any initiators
     * are later referenced that aren't in this list, trip
     * an assertion.
     */
    public TransactionInitPriorityQueue(int siteId) {
        m_siteId = siteId;
    }

    /**
     * Only return transaction state objects that are ready to run.
     */
    @Override
    public Long poll() {
        Long retval = null;
        if (m_state == QueueState.UNBLOCKED) {
            assert(checkQueueState() == QueueState.UNBLOCKED);
            retval = super.poll();
            assert(retval != null);
            m_txnsPopped++;
            m_lastTxnPopped = retval;
            // call this again to check
            checkQueueState();
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
    public boolean add(Long txnID) {
        boolean retval = super.add(txnID);
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
            StringBuilder msg = new StringBuilder();
            msg.append("Txn ordering deadlock (QUEUE) at site ").append(m_siteId).append(":\n");
            msg.append("   txn ").append(m_lastTxnPopped).append(" (");
            msg.append("   txn ").append(txnId).append(" (");
            System.err.print(msg.toString());
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

    /**
     * Return the largest confirmed txn id for the initiator given.
     * Used to figure out what to do after an initiator fails.
     * @param initiatorId The id of the initiator that has failed.
     */
    public long getNewestSafeTransactionForInitiator(int initiatorId) {
        return m_lastSafeTxnId;
    }

    public void shutdown() throws InterruptedException {
    }

    public QueueState getQueueState() {
        return m_state;
    }

    QueueState checkQueueState() {
        QueueState newState = QueueState.UNBLOCKED;
        Long ts = super.peek();
        if (ts == null) {
            newState = QueueState.BLOCKED_EMPTY;
        }
        else {
            if (ts > m_newestCandidateTransaction) {
                newState = QueueState.BLOCKED_ORDERING;
            }
            else {
//                if (ts > m_lastSafeTxnId) {
//                    newState = QueueState.BLOCKED_SAFETY;
//                }
            }
        }
        if (newState != m_state) {
            // THIS CODE IS HERE TO HANDLE A STATE CHANGE

            // note if we get non-empty but blocked
            if ((newState == QueueState.BLOCKED_ORDERING) || (newState == QueueState.BLOCKED_SAFETY)) {
                m_blockTime = System.currentTimeMillis();
            }
            if ((m_state == QueueState.BLOCKED_ORDERING) || (m_state == QueueState.BLOCKED_SAFETY)) {
                assert(m_state != QueueState.BLOCKED_EMPTY);
            }

            // if now blocked, send a heartbeat response
            if (newState == QueueState.BLOCKED_SAFETY) {
                assert(ts != null);
            }

            m_state = newState;
        }
        return m_state;
    }
}
