package edu.brown.hstore;

import java.util.Map;
import java.util.concurrent.PriorityBlockingQueue;

import org.apache.commons.collections15.map.ListOrderedMap;
import org.apache.log4j.Logger;
import org.voltdb.TransactionIdManager;
import org.voltdb.utils.EstTime;

import edu.brown.hstore.util.ThrottlingQueue;
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
public class TransactionInitPriorityQueue extends ThrottlingQueue<Long> {
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
    
    Long m_lastSeenTxnId = null;
    Long m_newestCandidateTransaction = -1l;
    long m_txnsPopped = 0;
    Long m_lastTxnPopped = 0l;
    long m_blockTime = 0;
    Long m_nextTxn = null;
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
              hstore_site.getHStoreConf().site.queue_dtxn_increase,
              hstore_site.getHStoreConf().site.queue_dtxn_increase_max
        );
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
//            assert(checkQueueState() == QueueState.UNBLOCKED);
            retval = super.poll();
            assert(retval != null);
        }
        if (d) LOG.debug(String.format("Partition %d poll() -> %s",
                         m_partitionId, 
                         (retval != null ? String.format("#%d/%d", retval, TransactionIdManager.getInitiatorIdFromTransactionId(retval)) : retval)));
        if (retval != null) {
            assert(m_nextTxn.equals(retval)) : 
                String.format("Partition %d - Next txn is #%d/%d but our poll returned txn #%d/%d\n%s",
                              m_partitionId,
                              m_nextTxn, TransactionIdManager.getInitiatorIdFromTransactionId(m_nextTxn),
                              retval, TransactionIdManager.getInitiatorIdFromTransactionId(retval),
                              this.toString());
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
    public synchronized Long peek() {
        Long retval = null;
        if (m_state == QueueState.UNBLOCKED) {
            assert(checkQueueState() == QueueState.UNBLOCKED);
            retval = super.peek();
            assert(retval != null);
        }
        if (d) LOG.debug(String.format("Partition %d peek() -> %s",
                         m_partitionId, 
                         (retval != null ? String.format("#%d/%d", retval, TransactionIdManager.getInitiatorIdFromTransactionId(retval)) : retval)));
        return retval;
    }

    /**
     * Drop data for unknown initiators. This is the only valid add interface.
     */
    @Override
    public synchronized boolean offer(Long txnId, boolean force) {
        assert(txnId != null);
        
        // Check whether this new txn is less than the current m_nextTxn
        // If it is and there is still time remaining before it is released,
        // then we'll switch and become the new next m_nextTxn
        if (m_nextTxn != null && txnId.compareTo(m_nextTxn) < 0) {
            checkQueueState();
            if (m_state != QueueState.UNBLOCKED) {
                if (d) LOG.debug(String.format("Partition %d Switching #%d/%d as new next txn [old=#%d/%d]",
                                 m_partitionId,
                                 txnId, TransactionIdManager.getInitiatorIdFromTransactionId(txnId),
                                 m_nextTxn, TransactionIdManager.getInitiatorIdFromTransactionId(m_nextTxn)));
                m_nextTxn = txnId;
            } else {
                if (d) LOG.debug(String.format("Partition %d offer(#%d/%d) -> %s",
                                 m_partitionId, 
                                 txnId, TransactionIdManager.getInitiatorIdFromTransactionId(txnId),
                                 "REJECTED"));
                return (false);
            }
        }
        
        boolean retval = super.offer(txnId, force);
        // update the queue state
        if (retval) checkQueueState();
        if (d) LOG.debug(String.format("Partition %d offer(#%d/%d) -> %s",
                         m_partitionId, 
                         txnId, TransactionIdManager.getInitiatorIdFromTransactionId(txnId), retval));
        return retval;
    }

    @Override
    public synchronized boolean remove(Object txnID) {
        boolean retval = super.remove(txnID);
        if (retval) checkQueueState();
        if (d) LOG.debug(String.format("Partition %d remove(#%d/%d) -> %s",
                         m_partitionId, 
                         txnID, TransactionIdManager.getInitiatorIdFromTransactionId((Long)txnID), retval));
        return retval;
    }

    /**
     * Update the information stored about the latest transaction
     * seen from each initiator. Compute the newest safe transaction id.
     */
    public synchronized long noteTransactionRecievedAndReturnLastSeen(Long txnId) {
        // this doesn't exclude dummy txnid but is also a sanity check
        assert(txnId != null);

        // we've decided that this can happen, and it's fine... just ignore it
        if (m_lastTxnPopped.compareTo(txnId) > 0) {
            if (d) {
                LOG.warn(String.format("Txn ordering deadlock at partition %d -> LastTxn: %d / NewTxn: %d",
                                       m_partitionId, m_lastTxnPopped, txnId));
                LOG.warn("LAST: " + TransactionIdManager.toString(m_lastTxnPopped));
                LOG.warn("NEW:  " + TransactionIdManager.toString(txnId));
            }
        }

        // update the latest transaction for the specified initiator
        if (m_lastSeenTxnId == null || txnId.compareTo(m_lastSeenTxnId) < 0) {
            if (t) LOG.trace("SET lastSeenTxnId = " + txnId);
            m_lastSeenTxnId = txnId;
        }

        // this minimum is the newest safe transaction to run
        // but you still need to check if a transaction has been confirmed
        //  by its initiator
        //  (note: this check is done when peeking/polling from the queue)
        if (t) LOG.trace("SET m_newestCandidateTransaction = " + m_lastSeenTxnId);
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
    Long getNewestSafeTransaction() {
        return m_newestCandidateTransaction;
    }

    public void shutdown() throws InterruptedException {
    }

    public QueueState getQueueState() {
        return m_state;
    }

    private QueueState checkQueueState() {
        QueueState newState = QueueState.UNBLOCKED;
        Long ts = super.peek();
        if (ts == null) {
            if (d) LOG.debug(String.format("Partition %d - Queue is empty.", m_partitionId));
            newState = QueueState.BLOCKED_EMPTY;
        }
        // Check whether can unblock now
        else if (ts == m_nextTxn && m_state != QueueState.UNBLOCKED) {
            if (EstTime.currentTimeMillis() < m_blockTime) {
                newState = QueueState.BLOCKED_SAFETY;
            } else if (d) {
                LOG.debug(String.format("Partition %d - Wait time for txn #%d has passed. Unblocking...",
                          m_partitionId, m_nextTxn));
            }
        }
        // This is a new txn and we should wait...
        else if (m_nextTxn == null || m_nextTxn != ts) {
            if (d) LOG.debug(String.format("Partition %d - Blocking next txn #%d for %d ms",
                             m_partitionId, ts, m_waitTime));
            newState = QueueState.BLOCKED_SAFETY;
            m_blockTime = EstTime.currentTimeMillis() + this.m_waitTime;
            m_nextTxn = ts;
        }
        
        if (newState != m_state) {
            m_state = newState;
            if (d) LOG.debug(String.format("Partition %d - State:%s / NextTxn:%s",
                             m_partitionId, m_state, m_nextTxn));
        }
        return m_state;
    }
    
    @Override
    public String toString() {
        Map<String, Object> m = new ListOrderedMap<String, Object>();
        m.put("PartitionId", m_partitionId);
        
        String labels[] = { "Next", "Last Popped", "Last Seen" };
        long txnids[] = null;
        synchronized (this) {
            txnids = new long[]{ m_nextTxn, m_lastTxnPopped, m_lastSeenTxnId };
        } // SYNCH
        for (int i = 0; i < labels.length; i++) {
            m.put(String.format("%s TxnId", labels[i]),
                  String.format("#%d/%d", txnids[i], TransactionIdManager.getInitiatorIdFromTransactionId(txnids[i])));
            
            if (i == 0) {
                m.put("Next Time Remaining", Math.max(0, System.currentTimeMillis() - m_blockTime));
            }
        } // FOR
        
        return (StringUtil.formatMaps(m));
    }
}
