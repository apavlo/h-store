package edu.mit.hstore.util;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import org.apache.log4j.Logger;

import edu.brown.hstore.Hstore;
import edu.brown.utils.LoggerUtil;
import edu.brown.utils.LoggerUtil.LoggerBoolean;
import edu.mit.hstore.HStoreSite;
import edu.mit.hstore.callbacks.BlockingCallback;
import edu.mit.hstore.callbacks.TransactionInitWrapperCallback;

public class TransactionQueue implements Runnable {
    public static final Logger LOG = Logger.getLogger(TransactionQueue.class);
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
    
    /**
     * contains one queue for every partition managed by this coordinator
     */
    private final TransactionInitPriorityQueue[] txn_queues;
    
    /**
     * the last txn ID that was executed for each partition
     */
    private final long[] last_txns;
    
    /**
     * indicates which partitions are currently executing a job
     */
    private final boolean[] working_partitions;
    
    /**
     * maps txn IDs to their callbacks
     */
    private final Map<Long, TransactionInitWrapperCallback> txn_callbacks = new HashMap<Long, TransactionInitWrapperCallback>();
    
    /**
     * Constructor
     * @param hstore_site
     */
    public TransactionQueue(HStoreSite hstore_site) {
        this.hstore_site = hstore_site;
        this.localPartitions = hstore_site.getLocalPartitionIds();
        assert(this.localPartitions.isEmpty() == false);
        
        int num_ids = this.localPartitions.size();
        this.txn_queues = new TransactionInitPriorityQueue[num_ids];
        this.last_txns = new long[num_ids];
        this.working_partitions = new boolean[num_ids];
        
        for (int p : this.localPartitions) {
            int offset = hstore_site.getLocalPartitionOffset(p);
            txn_queues[offset] = new TransactionInitPriorityQueue(hstore_site.getSiteId());
            last_txns[offset] = -1;
            working_partitions[offset] = false;
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
        while (true) {
            synchronized (this) {
                try {
                    wait();
                } catch (InterruptedException e) {
                    // Nothing...
                }
            } // SYNCH
            checkQueues();
        }
    }
    
    protected boolean isEmpty() {
        for (int i = 0; i < txn_queues.length; ++i) {
            if (txn_queues[i].isEmpty() == false) return (false);
        }
        return (true);
    }
    
    protected synchronized boolean checkQueues() {
        if (debug.get())
            LOG.debug("Checking queues");
        
        boolean txn_released = false;
        
        for (int offset = 0; offset < txn_queues.length; ++offset) {
            int partition = hstore_site.getLocalPartitionFromOffset(offset);
            if (working_partitions[offset]) {
                if (trace.get())
                    LOG.trace(String.format("Partition #%d is already executing a transaction. Skipping...", partition));
                continue;
            }
            TransactionInitPriorityQueue queue = txn_queues[offset];
            Long next_id = queue.poll();
            // If null, then there is nothing that is ready to run at this partition,
            // so we'll just skip to the next one
            if (next_id == null) {
                if (trace.get())
                    LOG.trace(String.format("Partition #%d does not have a transaction ready to run. Skipping...", partition));
                continue;
            }
            
            TransactionInitWrapperCallback callback = txn_callbacks.get(next_id);
            assert(callback != null) : "Unexpected null callback for txn #" + next_id;
            
            if (next_id < last_txns[offset]) {
                if (trace.get())
                    LOG.trace(String.format("The next id for Partition #%d is txn #%d but this is less than the previous txn #%d. Rejecting...",
                                            partition, next_id, last_txns[offset]));
                
                // First send back an ABORT message to the initiating HStoreSite
                try {
                    callback.abort(Hstore.Status.ABORT_REJECT);
                } catch (Throwable ex) {
                    throw new RuntimeException("Unexpected error when trying to abort txn #" + next_id, ex);
                }
                
                // Then mark the txn as done at all the partitions that we set as
                // as the current txn. Not sure how this will work...
                for (int p : callback.getPartitions()) {
                    int callback_offset = hstore_site.getLocalPartitionOffset(p);
                    if (next_id == last_txns[callback_offset]) {
                        this.done(next_id, p);
                    } else {
                        txn_queues[callback_offset].remove(next_id);
                    }
                } // FOR
                break;
            }

            // otherwise send the init request to the specified partition
            if (trace.get())
                LOG.trace(String.format("Good news! Partition #%d is ready to execute txn #%d! Invoking callback!", partition, next_id));
            last_txns[offset] = next_id;
            working_partitions[offset] = true;
            callback.run(partition);
            txn_released = true;
            
            // remove the callback when this partition is the last one to start the job
            if (callback.getCounter() == 0) {
                if (trace.get())
                    LOG.trace(String.format("All partitions needed by txn #%d have been claimed. Removing callback", next_id));
                txn_callbacks.remove(next_id);
            }
        } // FOR
        return txn_released;
    }
    
    /**
     * 
     * @param txn_id
     * @param partitions
     * @param callback
     */
    public synchronized void insert(long txn_id, Collection<Integer> partitions, TransactionInitWrapperCallback callback) {
        if (debug.get())
            LOG.debug(String.format("Adding new distributed txn #%d into queue [partitions=%s]", txn_id, partitions));
        
        txn_callbacks.put(txn_id, callback);
        boolean should_notify = false;
        for (Integer p : partitions) {
            if (this.localPartitions.contains(p) == false) continue;
            
            int offset = hstore_site.getLocalPartitionOffset(p.intValue());
            assert(offset >= 0 && offset < txn_queues.length) :
                String.format("Invalid offset %d for local partition #%d [length=%d]", offset, p, txn_queues.length);
            txn_queues[offset].noteTransactionRecievedAndReturnLastSeen(txn_id);
            txn_queues[offset].add(txn_id);
            if (!working_partitions[offset]) {
                should_notify = true;
            }
        } // FOR
        if (should_notify) {
            synchronized (this) {
                notifyAll();
            } // SYNCH
        }
    }
    
    /**
     * 
     * @param txn_id
     * @param partition
     */
    public void done(long txn_id, int partition) {
        if (debug.get())
            LOG.debug(String.format("Marking txn #%d as done on partition %d", txn_id, partition));
        
        int offset = hstore_site.getLocalPartitionOffset(partition);
        working_partitions[offset] = false;
        synchronized (this) {
            notifyAll();
        } // SYNCH
    }
}
