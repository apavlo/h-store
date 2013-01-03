package edu.brown.hstore;

import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.log4j.Logger;

import edu.brown.hstore.Hstoreservice.Status;
import edu.brown.hstore.conf.HStoreConf;
import edu.brown.hstore.txns.AbstractTransaction;
import edu.brown.interfaces.Shutdownable;
import edu.brown.logging.LoggerUtil;
import edu.brown.logging.LoggerUtil.LoggerBoolean;
import edu.brown.utils.MathUtil;
import edu.brown.utils.PartitionSet;
import edu.brown.utils.StringUtil;

/**
 * Special thread that can check whether the partition's queues are imbalanced 
 * (because of speed imbalance of the cores), and then shed transactions to
 * free up slots in the ClientInterface 
 * @author pavlo
 */
public class PartitionExecutorWorkloadShedder implements Runnable, Shutdownable {
    private static final Logger LOG = Logger.getLogger(PartitionExecutorWorkloadShedder.class);
    private static final LoggerBoolean debug = new LoggerBoolean(LOG.isDebugEnabled());
    private static final LoggerBoolean trace = new LoggerBoolean(LOG.isTraceEnabled());
    static {
        LoggerUtil.attachObserver(LOG, debug, trace);
    }
    
    private final HStoreSite hstore_site;
    private final HStoreConf hstore_conf;
    private final PartitionSet partitions;
    private final TransactionQueueManager queueManager;
    private boolean shutdown = false;
    private boolean initialized = false;
    
    private final int last_sizes[];
    private final PartitionExecutor executors[];
    private final TransactionInitPriorityQueue queues[];
    
    /**
     * Constructor
     * @param hstore_site
     */
    public PartitionExecutorWorkloadShedder(HStoreSite hstore_site) {
        this.hstore_site = hstore_site;
        this.hstore_conf = hstore_site.getHStoreConf();
        this.partitions = hstore_site.getLocalPartitionIds();
        this.queueManager = hstore_site.getTransactionQueueManager();
        
        int numPartitions = hstore_site.getCatalogContext().numberOfPartitions;
        this.last_sizes = new int[numPartitions];
        this.executors = new PartitionExecutor[numPartitions];
        this.queues = new TransactionInitPriorityQueue[numPartitions];
        
        Arrays.fill(this.last_sizes, 0);
    }
    
    protected void init() {
        if (debug.val)
            LOG.debug("Initializing " + this.getClass().getSimpleName());
        
        this.hstore_site.getThreadManager().registerProcessingThread();
        for (int partition : this.partitions.values()) {
            this.executors[partition] = hstore_site.getPartitionExecutor(partition);
            this.queues[partition] = this.queueManager.getInitQueue(partition);
        } // FOR
    }

    @Override
    public void run() {
        if (this.initialized == false) {
            this.init();
            this.initialized = true;
        }
         
        // Go through and get the queue size for each PartitionExecutor
        int total = 0;
        int sizes[] = new int[this.partitions.size()];
        int offset = 0;
        for (int partition : this.partitions.values()) {
            this.last_sizes[partition] = sizes[offset++] = this.queues[partition].size();
            total += this.last_sizes[partition];
        } // FOR
        
        // If a partition is two stdevs above the mean, then we will 
        // want to shed work from its queue
        if (total > 0) {
            double avg = MathUtil.arithmeticMean(sizes);
            double stdev = MathUtil.stdev(sizes);
            int threshold = (int)(avg + (stdev * 1.0));
            
            // *********************************** DEBUG ***********************************
            if (debug.val) {
                @SuppressWarnings("unchecked")
                Map<String, Object> maps[] = new Map[2];
                int idx = 0;
                
                maps[idx] = new LinkedHashMap<String, Object>();
                maps[idx].put("Total", total);
                maps[idx].put("Average", avg);
                maps[idx].put("Stdev", stdev);
                maps[idx].put("Threshold", threshold);
                
                maps[++idx] = new LinkedHashMap<String, Object>();
                for (int partition : this.partitions.values()) {
                    maps[idx].put("Partition " + partition, this.last_sizes[partition]);
                } // FOR
                
                LOG.debug("Queue Stats:\n" + StringUtil.formatMaps(maps));
            }
            // *********************************** DEBUG ***********************************
            
            for (int partition : this.partitions.values()) {
                if (threshold < this.last_sizes[partition]) {
                    this.shedWork(partition, this.last_sizes[partition] - threshold);
                }
            } // FOR
        }
    }
    
    protected void shedWork(int partition, int to_remove) {
        if (debug.val)
            LOG.debug(String.format("Attempting to shed workload from partition %d", partition));
        
        // Grab txns from the back of the queue and reject them
        int offset = this.last_sizes[partition] - to_remove;
        int idx = 0;
        for (AbstractTransaction ts : this.queues[partition]) {
            if (idx++ < offset) continue;
            if (debug.val)
                LOG.debug(String.format("Rejecting " + ts + " at partition " + partition));
            this.queueManager.lockQueueFinished(ts, Status.ABORT_REJECT, partition);
        } // FOR
    }

    
    @Override
    public boolean isShuttingDown() {
        return (this.shutdown == true);
    }
    
    @Override
    public void shutdown() {
        this.shutdown = true;
    }

    @Override
    public void prepareShutdown(boolean error) {
        // Nothing to do...
    }

}
