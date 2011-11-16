package edu.mit.hstore.util;

import java.util.Collection;
import java.util.Comparator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.PriorityBlockingQueue;

import org.apache.log4j.Logger;
import org.voltdb.TransactionIdManager;

import edu.brown.hstore.Hstore;
import edu.brown.logging.LoggerUtil;
import edu.brown.logging.LoggerUtil.LoggerBoolean;
import edu.mit.hstore.HStoreObjectPools;
import edu.mit.hstore.HStoreSite;
import edu.mit.hstore.callbacks.TransactionInitWrapperCallback;
import edu.mit.hstore.dtxn.LocalTransaction;

public class TransactionQueueManager implements Runnable {
    private static final Logger LOG = Logger.getLogger(TransactionQueueManager.class);
    private final static LoggerBoolean debug = new LoggerBoolean(LOG.isDebugEnabled());
    private final static LoggerBoolean trace = new LoggerBoolean(LOG.isTraceEnabled());
    static {
        LoggerUtil.attachObserver(LOG, debug, trace);
    }
    
    /**
     * the site that will send init requests to this coordinator
     */
    private final HStoreSite hstore_site;
    
    private final Collection<Integer> localPartitions;
    
    private final long wait_time;
    
    /**
     * contains one queue for every partition managed by this coordinator
     */
    private final TransactionInitPriorityQueue[] txn_queues;
    
    /**
     * The last txn ID that was executed for each partition
     * Our local partitions must be accurate, but we can be off for the remote ones
     */
    private final long[] last_txns;
    
    /**
     * indicates which partitions are currently executing a job
     */
    private final boolean[] working_partitions;
    
    /**
     * maps txn IDs to their callbacks
     */
    private final Map<Long, TransactionInitWrapperCallback> txn_callbacks = new ConcurrentHashMap<Long, TransactionInitWrapperCallback>();
    
    /**
     * Blocked Queue Comparator
     */
    private Comparator<LocalTransaction> blocked_comparator = new Comparator<LocalTransaction>() {
        @Override
        public int compare(LocalTransaction o0, LocalTransaction o1) {
            Long txnId0 = blocked_dtxn_release.get(o0);
            Long txnId1 = blocked_dtxn_release.get(o1);
            if (txnId0 == null && txnId1 == null) return (0);
            if (txnId0 == null) return (1);
            if (txnId1 == null) return (-1);
            if (txnId0.equals(txnId1) == false) return (txnId0.compareTo(txnId1));
            return (int)(o0.getClientHandle() - o1.getClientHandle());
        }
    };
    
    private ConcurrentHashMap<LocalTransaction, Long> blocked_dtxn_release = new ConcurrentHashMap<LocalTransaction, Long>();
    
    private PriorityBlockingQueue<LocalTransaction> blocked_dtxns = new PriorityBlockingQueue<LocalTransaction>(100, blocked_comparator);
    
    /**
     * Constructor
     * @param hstore_site
     */
    public TransactionQueueManager(HStoreSite hstore_site) {
        this.hstore_site = hstore_site;
        this.localPartitions = hstore_site.getLocalPartitionIds();
        assert(this.localPartitions.isEmpty() == false);
        
        Collection<Integer> allPartitions = hstore_site.getAllPartitionIds();
        int num_ids = allPartitions.size();
        this.txn_queues = new TransactionInitPriorityQueue[num_ids];
        this.working_partitions = new boolean[num_ids];
        this.last_txns = new long[allPartitions.size()];
        
        this.wait_time = hstore_site.getHStoreConf().site.txn_incoming_delay;
        for (int partition : allPartitions) {
            this.last_txns[partition] = -1;
            if (this.localPartitions.contains(partition)) {
                txn_queues[partition] = new TransactionInitPriorityQueue(hstore_site, partition, this.wait_time);
                working_partitions[partition] = false;
                hstore_site.getStartWorkloadObservable().addObserver(txn_queues[partition]);
            }
        } // FOR
        
        if (debug.get())
            LOG.debug(String.format("Created %d TransactionInitQueues for %s", num_ids, hstore_site.getSiteName()));
    }
    
    /**
     * Every time this thread gets waken up, it locks the queues, loops through the txn_queues, and looks at the lowest id in each queue.
     * If any id is lower than the last_txn id for that partition, it gets rejected and sent back to the caller.
     * Otherwise, the lowest txn_id is popped off and sent to the corresponding partition.
     * Then the thread unlocks the queues and goes back to sleep.
     * If all the partitions are now busy, the thread will wake up when one of them is finished.
     * Otherwise, it will wake up when something else gets added to a queue.
     */
    @Override
    public void run() {
        Thread self = Thread.currentThread();
        self.setName(HStoreSite.getThreadName(hstore_site, "queue"));
        if (hstore_site.getHStoreConf().site.cpu_affinity) {
            hstore_site.getThreadManager().registerProcessingThread();
        }
        
        if (debug.get())
            LOG.debug("Starting distributed transaction queue manager thread");
        
        TransactionIdManager idManager = hstore_site.getTransactionIdManager();
        long txn_id = -1;
        long last_id = -1;
        
        while (true) {
            synchronized (this) {
                try {
                    wait(this.wait_time);
                } catch (InterruptedException e) {
                    // Nothing...
                }
            } // SYNCH
            while (checkQueues()) {
                // Keep checking the queue as long as they have more stuff in there
                // for us to process
            }
            if (this.blocked_dtxns.isEmpty() == false) {
                txn_id = idManager.getLastTxnId();
                if (last_id == txn_id) txn_id = idManager.getNextUniqueTransactionId();
                checkBlockedDTXNs(txn_id);
                last_id = txn_id;
            }
        }
    }
    
    /**
     * 
     * @return
     */
    protected boolean checkQueues() {
        if (trace.get())
            LOG.trace("Checking queues");
        
        boolean txn_released = false;
        
        for (int partition : this.localPartitions) {
            TransactionInitWrapperCallback callback = null;
            Long next_id = null;
            int counter = -1;
            
//            synchronized (this.txn_queues[partition]) {
                if (working_partitions[partition]) {
                    if (trace.get())
                        LOG.trace(String.format("Partition #%d is already executing a transaction. Skipping...", partition));
                    continue;
                }
                TransactionInitPriorityQueue queue = txn_queues[partition];
                next_id = queue.poll();
                // If null, then there is nothing that is ready to run at this partition,
                // so we'll just skip to the next one
                if (next_id == null) {
                    if (trace.get())
                        LOG.trace(String.format("Partition #%d does not have a transaction ready to run. Skipping... [queueSize=%d]",
                                                partition, txn_queues[partition].size()));
                    continue;
                }
                
                callback = txn_callbacks.get(next_id);
                assert(callback != null) : "Unexpected null callback for txn #" + next_id;
                
                if (next_id < last_txns[partition]) {
                    if (trace.get()) 
                        LOG.trace(String.format("The next id for partition #%d is txn #%d but this is less than the previous txn #%d. Rejecting... [queueSize=%d]",
                                                partition, next_id, last_txns[partition], txn_queues[partition].size()));
                    this.rejectTransaction(next_id, callback, Hstore.Status.ABORT_RESTART, partition, last_txns[partition]);
                    continue;
                }
    
                // otherwise send the init request to the specified partition
                if (trace.get())
                    LOG.trace(String.format("Good news! Partition #%d is ready to execute txn #%d! Invoking callback!", partition, next_id));
                last_txns[partition] = next_id;
                working_partitions[partition] = true;
                callback.run(partition);
                counter = callback.getCounter();
//            } // SYNCH
            txn_released = true;
                
            // remove the callback when this partition is the last one to start the job
            if (counter == 0) {
                if (debug.get())
                    LOG.debug(String.format("All local partitions needed by txn #%d are ready. Removing callback", next_id));
                this.cleanupTransaction(next_id);
            }
        } // FOR
        return txn_released;
    }
    
    public void updateQueue() {
        synchronized (this) {
            notifyAll();
        }
    }
    
    private void cleanupTransaction(long txn_id) {
        TransactionInitWrapperCallback callback = this.txn_callbacks.remove(txn_id);
        
        if (callback != null) {
            for (int partition : callback.getPartitions()) {
                if (this.localPartitions.contains(partition) == false &&
                    this.last_txns[partition] < txn_id) {
                    if (debug.get())
                        LOG.debug(String.format("Marking txn #%d as last txn id for remote partition %d", txn_id, partition));
                    this.last_txns[partition] = txn_id;
                }
            } // FOR
            HStoreObjectPools.CALLBACKS_TXN_INITWRAPPER.returnObject(callback);
        }
    }
    
    private void rejectTransaction(long txn_id, TransactionInitWrapperCallback callback, Hstore.Status status, int reject_partition, long reject_txnId) {
        // First send back an ABORT message to the initiating HStoreSite
        try {
            callback.abort(status, reject_partition, reject_txnId);
        } catch (Throwable ex) {
            String msg = "Unexpected error when trying to abort txn #" + txn_id;
            throw new RuntimeException(msg, ex);
        }
        
        // Then mark the txn as done at all the partitions that we set as
        // as the current txn. Not sure how this will work...
        for (int partition : callback.getPartitions()) {
            if (this.localPartitions.contains(partition) == false) continue;
            txn_queues[partition].remove(txn_id);
            if (txn_id == last_txns[partition]) {
                this.finished(txn_id, status, partition);
            }
        }
        
        this.cleanupTransaction(txn_id);
    }
    
    /**
     * 
     * @param txn_id
     * @param partitions
     * @param callback
     */
    public boolean insert(long txn_id, Collection<Integer> partitions, TransactionInitWrapperCallback callback) {
        return this.insert(txn_id, partitions, callback, false);
    }
    
    /**
     * 
     * @param txn_id
     * @param partitions
     * @param callback
     * @param force
     * @return
     */
    public boolean insert(long txn_id, Collection<Integer> partitions, TransactionInitWrapperCallback callback, boolean force) {
        if (debug.get())
            LOG.debug(String.format("Adding new distributed txn #%d into queue [force=%s, partitions=%s]", txn_id, force, partitions));
        
        txn_callbacks.put(txn_id, callback);
        boolean should_notify = false;
        boolean ret = true;
        for (Integer partition : partitions) {
//            synchronized (this.txn_queues[partition]) {
                // We can pre-emptively check whether this txnId is greater than
                // the largest one that we know about at a remote partition
                if (this.localPartitions.contains(partition) == false) {
                    if (this.last_txns[partition] > txn_id) {
                        if (trace.get()) 
                            LOG.trace(String.format("The last txn for remote partition is #%d but this is greater than our txn #%d. Rejecting...",
                                                    partition, this.last_txns[partition], txn_id));
                        this.rejectTransaction(txn_id, callback, Hstore.Status.ABORT_RESTART, partition, this.last_txns[partition]);
                        ret = false;
                        break;
                    }
                    continue;
                }
                
                long next_safe = txn_queues[partition].noteTransactionRecievedAndReturnLastSeen(txn_id);
                if (next_safe > txn_id) {
                    if (trace.get()) 
                        LOG.trace(String.format("The next safe id for partition #%d is txn #%d but this is less than our new txn #%d. Rejecting...",
                                                partition, next_safe, txn_id));
                    this.rejectTransaction(txn_id, callback, Hstore.Status.ABORT_RESTART, partition, next_safe);
                    ret = false;
                    break;
                } else if (txn_queues[partition].offer(txn_id, false) == false) {
                    if (trace.get()) 
                        LOG.trace(String.format("The DTXN queue partition #%d is overloaded. Throttling txn #%d",
                                                partition, next_safe, txn_id));
                    this.rejectTransaction(txn_id, callback, Hstore.Status.ABORT_THROTTLED, partition, next_safe);
                    ret = false;
                    break;
                } else if (!working_partitions[partition]) {
                    should_notify = true;
                }
//            } // SYNCH
            
            if (trace.get()) 
                LOG.trace(String.format("Added txn #%d to queue for partition %d [working=%s, queueSize=%d]",
                                        txn_id, partition, this.working_partitions[partition], this.txn_queues[partition].size()));
        } // FOR
        if (should_notify) {
            synchronized (this) {
                notifyAll();
            } // SYNCH
        }
        return (ret);
    }
    
    /**
     * 
     * @param txn_id
     * @param partition
     */
    public void finished(long txn_id, Hstore.Status status, int partition) {
        if (debug.get())
            LOG.debug(String.format("Marking txn #%d as finished on partition %d [status=%s, basePartition=%d]",
                                    txn_id, partition, status,
                                    TransactionIdManager.getInitiatorIdFromTransactionId(txn_id)));
        
        // Always remove it from this partition's queue
        if (this.txn_queues[partition].remove(txn_id) == false) {
            // Then free up the partition if it was actually running
            if (last_txns[partition] == txn_id) {
                working_partitions[partition] = false;
            }
            synchronized (this) {
                notifyAll();
            } // SYNCH
        }
    }
    
    protected boolean isEmpty() {
        for (int i = 0; i < txn_queues.length; ++i) {
            if (txn_queues[i].isEmpty() == false) return (false);
        }
        return (true);
    }

    public long getLastTransaction(int partition) {
        return (this.last_txns[partition]);
    }
    public Long getCurrentTransaction(int partition) {
        if (working_partitions[partition]) {
            return (this.last_txns[partition]);
        }
        return (null);
    }
    
    public int getQueueSize(int partition) {
        return (this.txn_queues[partition].size());
    }
    
    public TransactionInitPriorityQueue getQueue(int partition) {
        return (this.txn_queues[partition]);
    }
    
    public TransactionInitWrapperCallback getCallback(long txn_id) {
        return (this.txn_callbacks.get(txn_id));
    }
    
    /**
     * 
     * @param partition
     * @param txn_id
     */
    public synchronized void markAsLastTxnId(int partition, long txn_id) {
        if (last_txns[partition] < txn_id) {
            if (debug.get())
                LOG.debug(String.format("Marking txn #%d as last txn id for remote partition %d", txn_id, partition));
            last_txns[partition] = txn_id;
        }
    }
    
    /**
     * 
     * @param ts
     * @param partition
     * @param txn_id
     */
    public void queueBlockedDTXN(LocalTransaction ts, int partition, long txn_id) {
        if (debug.get()) 
            LOG.debug(String.format("Blocking %s until after a txn greater than #%d is created for partition %d",
                                                 ts, txn_id, partition));
        if (this.blocked_dtxn_release.putIfAbsent(ts, txn_id) != null) {
            Long other_txn_id = this.blocked_dtxn_release.get(ts);
            if (other_txn_id != null && other_txn_id.longValue() < txn_id) {
                this.blocked_dtxn_release.put(ts, txn_id);
            }
        } else {
            this.blocked_dtxns.offer(ts);
        }
        if (this.localPartitions.contains(partition) == false) {
            this.markAsLastTxnId(partition, txn_id);
        }
    }
    
    /**
     * 
     * @param last_txn_id
     */
    private void checkBlockedDTXNs(long last_txn_id) {
        if (debug.get() && this.blocked_dtxns.isEmpty() == false)
            LOG.debug(String.format("Checking whether we can release %d blocked dtxns [lastTxnId=%d]", this.blocked_dtxns.size(), last_txn_id));
        
        while (this.blocked_dtxns.isEmpty() == false) {
            LocalTransaction ts = this.blocked_dtxns.peek();
            Long releaseTxnId = this.blocked_dtxn_release.get(ts);
            if (releaseTxnId == null) {
                if (debug.get()) LOG.warn("Missing release TxnId for " + ts);
                this.blocked_dtxns.remove();
                continue;
            }
            if (releaseTxnId < last_txn_id) {
                if (debug.get())
                    LOG.debug(String.format("Releasing blocked %s because the lastest txn was #%d [release=%d]",
                                            ts, last_txn_id, releaseTxnId));
                this.blocked_dtxns.remove();
                this.blocked_dtxn_release.remove(ts);
                hstore_site.transactionRestart(ts, Hstore.Status.ABORT_RESTART);
//                hstore_site.transactionRequeue(p.getSecond());
            } else break;
        } // WHILE
    }
}
