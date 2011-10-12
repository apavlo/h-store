package edu.mit.hstore.util;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import org.apache.log4j.Logger;

import edu.brown.utils.LoggerUtil;
import edu.brown.utils.LoggerUtil.LoggerBoolean;
import edu.mit.hstore.HStoreSite;
import edu.mit.hstore.callbacks.RemoteTransactionInitCallback;

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
    
    /**
     * contains one queue for every partition managed by this coordinator
     */
    private TransactionInitPriorityQueue[] txn_queues;
    
    /**
     * the last txn ID that was executed for each partition
     */
    private long[] last_txns;
    
    /**
     * indicates which partitions are currently executing a job
     */
    private boolean[] working_partitions;
    
    /**
     * maps txn IDs to their callbacks
     */
    private Map<Long, RemoteTransactionInitCallback> txn_callbacks;
    
    public TransactionQueue(HStoreSite site) {
        this.hstore_site = site;
        Collection<Integer> ids = hstore_site.getLocalPartitionIds();
        int num_ids = ids.size();

        this.txn_queues = new TransactionInitPriorityQueue[num_ids];
        this.last_txns = new long[num_ids];
        this.working_partitions = new boolean[num_ids];
        this.txn_callbacks = new HashMap<Long, RemoteTransactionInitCallback>();
        
        for (int p : ids) {
            int offset = HStoreSite.LOCAL_PARTITION_OFFSETS[p];
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
        while (true) {
            try {
                wait();
            } catch (InterruptedException e) {
            }
            checkQueues();
        }
    }
    
    protected boolean isEmpty() {
        for (int i = 0; i < txn_queues.length; ++i) {
            if (txn_queues[i].isEmpty() == false) return (false);
        }
        return (true);
    }
    
    protected synchronized void checkQueues() {
        if (debug.get())
            LOG.debug("Checking queues");
        
        for (int offset = 0; offset < txn_queues.length; ++offset) {
            int partition = HStoreSite.LOCAL_PARTITION_REVERSE[offset];
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
            
            if (next_id < last_txns[offset]) {
                assert(false) : "FIXME";
                // return an ABORT_RETRY message to client
            }

            // otherwise send the init request to the specified partition
            if (trace.get())
                LOG.trace(String.format("Good news! Partition #%d is ready to execute txn #%d! Invoking callback!", partition, next_id));
            last_txns[offset] = next_id;
            working_partitions[offset] = true;
            RemoteTransactionInitCallback callback = txn_callbacks.get(next_id);
            callback.run(partition);
            
            // remove the callback when this partition is the last one to start the job
            if (callback.getCounter() == 0) {
                if (trace.get())
                    LOG.trace(String.format("All partitions needed by txn #%d have been claimed. Removing callback", next_id));
                txn_callbacks.remove(next_id);
            }
        }
    }
    
    public synchronized void insert(long txn_id, Collection<Integer> partitions, RemoteTransactionInitCallback callback) {
        if (debug.get())
            LOG.debug(String.format("Adding new distributed txn #%d into queue [partitions=%s]", txn_id, partitions));
        
        txn_callbacks.put(txn_id, callback);
        boolean should_notify = false;
        for (int p : partitions) {
            int offset = HStoreSite.LOCAL_PARTITION_OFFSETS[p];
            txn_queues[offset].noteTransactionRecievedAndReturnLastSeen(txn_id);
            txn_queues[offset].add(txn_id);
            if (!working_partitions[offset]) {
                should_notify = true;
            }
        } // FOR
        if (should_notify) notifyAll();
    }
    
    public void done(long txn_id, int partition) {
        if (debug.get())
            LOG.debug(String.format("Marking txn #%d as done on partition %d", txn_id, partition));
        
        int offset = HStoreSite.LOCAL_PARTITION_OFFSETS[partition];
        working_partitions[offset] = false;
        notifyAll();
    }
}
